"""Tests for `pod accounts` subcommand (list / release / evict-orphans)."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from pod import accounts, cli


class _Harness(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pod-accountscmd-test-"))
        self._orig_claude_dir = accounts.CLAUDE_DIR
        self._orig_lease_dir = accounts.LEASE_DIR
        self._orig_meta = accounts.LEASE_META_LOCK
        accounts.CLAUDE_DIR = self.tmp / ".claude"
        accounts.LEASE_DIR = accounts.CLAUDE_DIR / "pod-account-leases"
        accounts.LEASE_META_LOCK = accounts.LEASE_DIR / ".lock"
        accounts.CLAUDE_DIR.mkdir(parents=True)
        self._orig_pod_dir = cli.POD_DIR
        self._orig_project_dir = cli.PROJECT_DIR
        self._orig_agents_dir = cli.AGENTS_DIR
        cli.POD_DIR = self.tmp / ".pod"
        cli.PROJECT_DIR = self.tmp
        cli.AGENTS_DIR = cli.POD_DIR / "agents"
        cli.POD_DIR.mkdir(parents=True)
        cli.AGENTS_DIR.mkdir(parents=True)

    def tearDown(self) -> None:
        accounts.CLAUDE_DIR = self._orig_claude_dir
        accounts.LEASE_DIR = self._orig_lease_dir
        accounts.LEASE_META_LOCK = self._orig_meta
        cli.POD_DIR = self._orig_pod_dir
        cli.PROJECT_DIR = self._orig_project_dir
        cli.AGENTS_DIR = self._orig_agents_dir
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_credentials(self, num: int, label: str):
        p = accounts.CLAUDE_DIR / f"credentials{num}.json"
        p.write_text(json.dumps({
            "accountLabel": label,
            "claudeAiOauth": {
                "accessToken": "tok",
                "expiresAt": "2026-12-31T00:00:00Z",
            },
        }))

    def _write_agent(self, short_id: str, status: str = "running"):
        s = cli.AgentState(short_id=short_id, pid=os.getpid(), status=status)
        s.write()

    def _make_lease(self, label: str, short_id: str,
                     project_dir: str = "/tmp/proj"):
        with accounts.lease_critical_section():
            accounts.try_acquire_lease(label, short_id, project_dir=project_dir)

    def _run(self, action: str, **kwargs) -> str:
        args = SimpleNamespace(accounts_action=action, **kwargs)
        buf = io.StringIO()
        with redirect_stdout(buf):
            try:
                cli.cmd_accounts({}, args)
            except SystemExit as e:
                buf.write(f"\n<exit {e.code}>")
        return buf.getvalue()


class ListTests(_Harness):
    def test_empty(self):
        out = self._run("list")
        self.assertIn("no accounts found", out)

    def test_all_free(self):
        self._write_credentials(2, "alpha")
        self._write_credentials(3, "beta")
        out = self._run("list")
        self.assertIn("alpha", out)
        self.assertIn("beta", out)
        self.assertIn("(free)", out)

    def test_held_lease_shows_owner_and_pid(self):
        self._write_credentials(4, "lean-fro")
        self._write_agent("agent1234", status="running")
        self._make_lease("lean-fro", "agent1234", "/Users/kim/projects/x")
        out = self._run("list")
        self.assertIn("lean-fro", out)
        self.assertIn("agent1234", out)
        self.assertIn("/Users/kim/projects/x", out)
        # No orphan marker
        self.assertNotIn("*", out.replace("\n", " "))

    def test_orphan_lease_is_marked(self):
        self._write_credentials(4, "lean-fro")
        # No matching agent state.
        self._make_lease("lean-fro", "deadbeef")
        out = self._run("list")
        self.assertIn("deadbeef*", out)
        self.assertIn("orphan", out)

    def test_lease_with_no_account_file_surfaces(self):
        self._write_agent("agent1234", status="running")
        self._make_lease("ghost-account", "agent1234")
        out = self._run("list")
        self.assertIn("ghost-account", out)
        self.assertIn("no account file", out)


class ReleaseTests(_Harness):
    def test_release_held_by_dead_agent(self):
        self._write_credentials(4, "lean-fro")
        self._make_lease("lean-fro", "deadbeef")
        out = self._run("release", label="lean-fro", force=False)
        self.assertIn("Released 'lean-fro'", out)
        self.assertEqual(accounts.list_leases(), [])

    def test_release_refuses_live_agent_without_force(self):
        self._write_credentials(4, "lean-fro")
        self._write_agent("agent1234", status="running")
        self._make_lease("lean-fro", "agent1234")
        out = self._run("release", label="lean-fro", force=False)
        self.assertIn("Refusing to release", out)
        self.assertIn("exit 1", out)
        # Lease still there
        self.assertEqual(len(accounts.list_leases()), 1)

    def test_release_force_overrides_live_check(self):
        self._write_credentials(4, "lean-fro")
        self._write_agent("agent1234", status="running")
        self._make_lease("lean-fro", "agent1234")
        out = self._run("release", label="lean-fro", force=True)
        self.assertIn("Released 'lean-fro'", out)
        self.assertEqual(accounts.list_leases(), [])

    def test_release_missing_label(self):
        out = self._run("release", label="nonexistent", force=False)
        self.assertIn("No lease held", out)


class EvictOrphansTests(_Harness):
    def test_evict_when_no_live_agents(self):
        self._make_lease("alpha", "dead1")
        self._make_lease("beta", "dead2")
        out = self._run("evict-orphans")
        self.assertIn("Evicted 2", out)
        self.assertEqual(accounts.list_leases(), [])

    def test_evict_keeps_live(self):
        self._write_agent("alive1", status="running")
        self._make_lease("alpha", "alive1")
        self._make_lease("beta", "dead1")
        out = self._run("evict-orphans")
        self.assertIn("Evicted 1", out)
        labels = [l.label for l in accounts.list_leases()]
        self.assertEqual(labels, ["alpha"])

    def test_no_orphans(self):
        self._write_agent("alive1", status="running")
        self._make_lease("alpha", "alive1")
        out = self._run("evict-orphans")
        self.assertIn("No orphan leases", out)


class UnknownActionTests(_Harness):
    def test_unknown_action_exits_2(self):
        out = self._run("bogus-action")
        self.assertIn("Unknown action", out)
        self.assertIn("exit 2", out)


if __name__ == "__main__":
    unittest.main()
