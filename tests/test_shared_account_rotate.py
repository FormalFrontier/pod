"""Tests for pod's shared-mode credential rotation.

When pod defers account selection to an external manager (signalled by
``~/.claude/.current-account``), it must drive that manager itself — a
``systemd --user`` timer can wedge on an unprivileged host — rather than
sit in ``waiting_quota`` forever. These cover the gating, rate-limiting,
and wedge-heal helpers in ``pod.cli``.
"""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import accounts, cli


class _Harness(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pod-rotate-test-"))
        # accounts.current_account() reads accounts.CLAUDE_DIR/.current-account
        self._orig_claude_dir = accounts.CLAUDE_DIR
        accounts.CLAUDE_DIR = self.tmp / ".claude"
        accounts.CLAUDE_DIR.mkdir(parents=True)
        # cli stamp/lock live under POD_DIR
        self._orig_pod_dir = cli.POD_DIR
        self._orig_lock = cli.SHARED_ROTATE_LOCK_PATH
        self._orig_stamp = cli.SHARED_ROTATE_STAMP_PATH
        cli.POD_DIR = self.tmp / ".pod"
        cli.POD_DIR.mkdir(parents=True)
        cli.SHARED_ROTATE_LOCK_PATH = cli.POD_DIR / "shared-account-rotate.lock"
        cli.SHARED_ROTATE_STAMP_PATH = cli.POD_DIR / "shared-account-rotate.stamp"
        # A real, on-disk manager command (its contents never run — subprocess
        # is mocked) so the exists() check passes.
        self.manager = self.tmp / ".claude" / "swap-account"
        self.manager.write_text("#!/bin/sh\n")
        self.config = {"quota": {"account_manager_cmd": str(self.manager)}}

    def tearDown(self) -> None:
        import shutil
        accounts.CLAUDE_DIR = self._orig_claude_dir
        cli.POD_DIR = self._orig_pod_dir
        cli.SHARED_ROTATE_LOCK_PATH = self._orig_lock
        cli.SHARED_ROTATE_STAMP_PATH = self._orig_stamp
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _mark_shared(self) -> None:
        (accounts.CLAUDE_DIR / ".current-account").write_text("3\n")


class ManagerCmdGatingTests(_Harness):
    def test_none_without_current_account_marker(self):
        # No .current-account → pod manages accounts itself → don't invoke.
        self.assertIsNone(cli._shared_account_manager_cmd(self.config))

    def test_resolves_when_shared_and_present(self):
        self._mark_shared()
        self.assertEqual(
            cli._shared_account_manager_cmd(self.config), str(self.manager))

    def test_none_when_command_missing_on_disk(self):
        self._mark_shared()
        self.manager.unlink()
        self.assertIsNone(cli._shared_account_manager_cmd(self.config))

    def test_disabled_by_empty_config(self):
        self._mark_shared()
        self.assertIsNone(
            cli._shared_account_manager_cmd({"quota": {"account_manager_cmd": ""}}))

    def test_default_path_used_when_config_absent(self):
        # No [quota] override → defaults to ~/.claude/swap-account, resolved
        # against the (patched) accounts.CLAUDE_DIR home.
        self._mark_shared()
        with mock.patch("os.path.expanduser", return_value=str(self.manager)):
            self.assertEqual(cli._shared_account_manager_cmd({}), str(self.manager))


class RotateTests(_Harness):
    def test_noop_outside_shared_mode(self):
        with mock.patch.object(cli.subprocess, "run") as run:
            cli._maybe_rotate_shared_account(self.config, min_interval=60)
        run.assert_not_called()

    def test_runs_manager_best_in_shared_mode(self):
        self._mark_shared()
        with mock.patch.object(cli.subprocess, "run") as run, \
                mock.patch.object(cli, "_heal_wedged_user_timers"):
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            cli._maybe_rotate_shared_account(self.config, min_interval=60)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0], [str(self.manager), "best"])
        self.assertTrue(cli.SHARED_ROTATE_STAMP_PATH.exists())

    def test_rate_limited_within_interval(self):
        self._mark_shared()
        with mock.patch.object(cli.subprocess, "run") as run, \
                mock.patch.object(cli, "_heal_wedged_user_timers"):
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            cli._maybe_rotate_shared_account(self.config, min_interval=600)
            cli._maybe_rotate_shared_account(self.config, min_interval=600)
        self.assertEqual(run.call_count, 1)  # second call short-circuited

    def test_runs_again_after_interval_elapses(self):
        self._mark_shared()
        with mock.patch.object(cli.subprocess, "run") as run, \
                mock.patch.object(cli, "_heal_wedged_user_timers"):
            run.return_value = subprocess.CompletedProcess([], 0, "", "")
            cli._maybe_rotate_shared_account(self.config, min_interval=600)
            # Backdate the stamp beyond the window.
            cli.SHARED_ROTATE_STAMP_PATH.write_text("1.0\n")
            cli._maybe_rotate_shared_account(self.config, min_interval=600)
        self.assertEqual(run.call_count, 2)

    def test_manager_failure_is_swallowed_and_stamped(self):
        self._mark_shared()
        with mock.patch.object(
                cli.subprocess, "run",
                side_effect=subprocess.TimeoutExpired("swap-account", 180)), \
                mock.patch.object(cli, "log"):
            cli._maybe_rotate_shared_account(self.config, min_interval=60)
        # Stamp is written even on failure so a hung manager can't be retried
        # every 60s across the fleet.
        self.assertTrue(cli.SHARED_ROTATE_STAMP_PATH.exists())


class WedgeDetectionTests(_Harness):
    def _run_returning(self, stdout: str, rc: int = 0):
        return subprocess.CompletedProcess([], rc, stdout, "")

    def test_healthy_realtime_timer_not_wedged(self):
        out = ("ActiveState=active\n"
               "NextElapseUSecRealtime=Wed 2026-07-08 04:00:00 UTC\n"
               "NextElapseUSecMonotonic=0\n")
        with mock.patch.object(cli.subprocess, "run",
                               return_value=self._run_returning(out)):
            self.assertFalse(cli._user_rotate_timers_wedged())

    def test_infinity_monotonic_and_no_realtime_is_wedged(self):
        out = ("ActiveState=active\n"
               "NextElapseUSecRealtime=\n"
               "NextElapseUSecMonotonic=infinity\n")
        with mock.patch.object(cli.subprocess, "run",
                               return_value=self._run_returning(out)):
            self.assertTrue(cli._user_rotate_timers_wedged())

    def test_inactive_timer_ignored(self):
        out = ("ActiveState=inactive\n"
               "NextElapseUSecRealtime=\n"
               "NextElapseUSecMonotonic=infinity\n")
        with mock.patch.object(cli.subprocess, "run",
                               return_value=self._run_returning(out)):
            self.assertFalse(cli._user_rotate_timers_wedged())

    def test_missing_systemctl_is_not_wedged(self):
        with mock.patch.object(cli.subprocess, "run",
                               side_effect=FileNotFoundError):
            self.assertFalse(cli._user_rotate_timers_wedged())

    def test_heal_reexecs_only_when_wedged(self):
        with mock.patch.object(cli, "_user_rotate_timers_wedged",
                               return_value=True), \
                mock.patch.object(cli.subprocess, "run") as run, \
                mock.patch.object(cli, "log"):
            cli._heal_wedged_user_timers()
        argvs = [c.args[0] for c in run.call_args_list]
        self.assertIn(["systemctl", "--user", "daemon-reexec"], argvs)

    def test_heal_noop_when_healthy(self):
        with mock.patch.object(cli, "_user_rotate_timers_wedged",
                               return_value=False), \
                mock.patch.object(cli.subprocess, "run") as run:
            cli._heal_wedged_user_timers()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
