"""Tests for cli.acquire_backend — the new lease-aware backend picker.

These tests exercise the integration between cli.acquire_backend and
pod.accounts: enumeration, atomic lease + revalidation, sticky leases,
mirror-on-acquire, resume pinning. The accounts module's internals are
tested separately in test_accounts.py.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import accounts, cli


def _quota_cp(stdout: str = "opus", returncode: int = 0):
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout + "\n", stderr="")


def _config(backend: str = "auto", *,
             prefer: str = "claude",
             accepted_models: list[str] | None = None,
             codex_model: str = "gpt-5.4") -> dict:
    cfg = {
        "agent": {
            "backend": backend,
            "auto": {"prefer": prefer},
            "claude": {
                "model": "opus",
                "quota_check": "/bin/claude-quota",
                "quota_check_required": True,
                "isolated_config": True,
            },
            "codex": {
                "model": codex_model,
                "quota_check": "/bin/codex-quota",
                "quota_check_required": True,
                "isolated_config": True,
            },
        }
    }
    if accepted_models is not None:
        cfg["agent"]["claude"]["accepted_models"] = accepted_models
    return cfg


class _Harness(unittest.TestCase):
    """Sets up a temp ~/.claude tree, mocks the keychain, and reroutes
    POD_DIR/PROJECT_DIR so the meta-lock and state files land in tmp."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pod-acquire-test-"))
        # Override accounts module globals
        self._orig_claude_dir = accounts.CLAUDE_DIR
        self._orig_lease_dir = accounts.LEASE_DIR
        self._orig_meta = accounts.LEASE_META_LOCK
        accounts.CLAUDE_DIR = self.tmp / ".claude"
        accounts.LEASE_DIR = accounts.CLAUDE_DIR / "pod-account-leases"
        accounts.LEASE_META_LOCK = accounts.LEASE_DIR / ".lock"
        accounts.CLAUDE_DIR.mkdir(parents=True)
        # Override cli paths
        self._orig_pod_dir = cli.POD_DIR
        self._orig_project_dir = cli.PROJECT_DIR
        self._orig_force_quota_file = cli.FORCE_QUOTA_FILE
        self._orig_agents_dir = cli.AGENTS_DIR
        cli.POD_DIR = self.tmp / ".pod"
        cli.PROJECT_DIR = self.tmp
        cli.FORCE_QUOTA_FILE = cli.POD_DIR / "force-quota"
        cli.AGENTS_DIR = cli.POD_DIR / "agents"
        cli.POD_DIR.mkdir(parents=True)
        cli.AGENTS_DIR.mkdir(parents=True)
        # Mock keychain — we don't want test pods to touch the real one
        self._kc: dict[str, str] = {}
        self._kc_read = mock.patch.object(
            accounts, "_keychain_read",
            side_effect=lambda s: self._kc.get(s))
        self._kc_write = mock.patch.object(
            accounts, "_keychain_write",
            side_effect=lambda s, b: (self._kc.__setitem__(s, b) or True))
        self._kc_read.start()
        self._kc_write.start()
        # Stub _reload_config_value to read accepted_models / model from
        # the in-memory config (not from disk, which isn't a real
        # config.toml in this harness).
        self._cfg_for_reload: dict = {}
        self._reload = mock.patch.object(
            cli, "_reload_config_value",
            side_effect=self._fake_reload)
        self._reload.start()
        self.addCleanup(self.tearDown_helpers)

    def _fake_reload(self, *keys, default=None):
        cur: object = self._cfg_for_reload
        for k in keys:
            if not isinstance(cur, dict) or k not in cur:
                return default
            cur = cur[k]
        return cur

    def tearDown_helpers(self):
        self._kc_read.stop()
        self._kc_write.stop()
        self._reload.stop()

    def tearDown(self) -> None:
        accounts.CLAUDE_DIR = self._orig_claude_dir
        accounts.LEASE_DIR = self._orig_lease_dir
        accounts.LEASE_META_LOCK = self._orig_meta
        cli.POD_DIR = self._orig_pod_dir
        cli.PROJECT_DIR = self._orig_project_dir
        cli.FORCE_QUOTA_FILE = self._orig_force_quota_file
        cli.AGENTS_DIR = self._orig_agents_dir
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # --- Helpers -----------------------------------------------------------

    def _write_credentials(self, num: int, label: str,
                            expires: str = "2026-12-31T00:00:00Z"):
        p = accounts.CLAUDE_DIR / f"credentials{num}.json"
        p.write_text(json.dumps({
            "accountLabel": label,
            "claudeAiOauth": {
                "accessToken": "sk-ant-test-AAAAAAAAAAAAAAAAAAAA",
                "expiresAt": expires,
            },
        }))
        return p

    def _make_state(self, short_id: str = "agent1234") -> cli.AgentState:
        state = cli.AgentState(short_id=short_id, pid=os.getpid(),
                                status="starting")
        # Write to AGENTS_DIR so orphan-lease eviction inside
        # acquire_backend's meta-lock recognises this short_id as live
        # and doesn't reap a lease we just acquired for a sibling test
        # agent.
        state.write()
        return state

    def _set_cfg(self, cfg: dict):
        """Mirror cfg into the reload-stub view so reload() pulls from it."""
        self._cfg_for_reload = cfg


# --- Single-claude-account happy path --------------------------------------


class SingleAccountTests(_Harness):
    def test_opus_account_acquires_and_leases(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account",
                                return_value="opus"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.backend, "claude")
        self.assertEqual(sel.label, "lean-fro")
        self.assertEqual(sel.model, "opus")
        self.assertEqual(sel.account_num, 4)
        # State + lease updated
        self.assertEqual(state.account_label, "lean-fro")
        self.assertGreater(state.lease_acquired_at, 0)
        leases = accounts.list_leases()
        self.assertEqual([l.label for l in leases], ["lean-fro"])

    def test_no_accounts_returns_none(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        sel = cli.acquire_backend(
            cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNone(sel)
        self.assertEqual(state.account_label, "")

    def test_sonnet_only_account_rejected_when_opus_required(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account",
                                return_value="sonnet"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNone(sel)

    def test_sonnet_account_accepted_when_listed(self):
        cfg = _config("claude", accepted_models=["opus", "sonnet"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account",
                                return_value="sonnet"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "lean-fro")
        self.assertEqual(sel.model, "sonnet")


# --- Multi-account contention ------------------------------------------------


class MultiAccountTests(_Harness):
    def test_two_agents_pick_different_accounts(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        self._write_credentials(3, "beta")
        state1 = self._make_state("agent1111")
        state2 = self._make_state("agent2222")
        cdir1 = accounts.agent_claude_config_dir(cli.POD_DIR, state1.short_id)
        cdir2 = accounts.agent_claude_config_dir(cli.POD_DIR, state2.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state1.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state2.short_id)
        with mock.patch.object(accounts, "probe_account",
                                return_value="opus"):
            sel1 = cli.acquire_backend(cfg, state=state1, claude_config_dir=cdir1)
            sel2 = cli.acquire_backend(cfg, state=state2, claude_config_dir=cdir2)
        self.assertIsNotNone(sel1)
        self.assertIsNotNone(sel2)
        self.assertNotEqual(sel1.label, sel2.label)
        held = sorted(l.label for l in accounts.list_leases())
        self.assertEqual(held, ["alpha", "beta"])

    def test_third_agent_waits_when_all_leased(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        state1 = self._make_state("agent1111")
        state2 = self._make_state("agent2222")
        cdir1 = accounts.agent_claude_config_dir(cli.POD_DIR, state1.short_id)
        cdir2 = accounts.agent_claude_config_dir(cli.POD_DIR, state2.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state1.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state2.short_id)
        with mock.patch.object(accounts, "probe_account",
                                return_value="opus"):
            sel1 = cli.acquire_backend(cfg, state=state1, claude_config_dir=cdir1)
            sel2 = cli.acquire_backend(cfg, state=state2, claude_config_dir=cdir2)
        self.assertIsNotNone(sel1)
        self.assertIsNone(sel2)

    def test_codex_picked_when_no_claude_in_auto(self):
        cfg = _config("auto", prefer="claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # Claude exhausted, codex available
        def fake_probe(cmd, label, force=False):
            return None
        with mock.patch.object(accounts, "probe_account",
                                side_effect=fake_probe), \
             mock.patch.object(accounts, "probe_codex",
                                return_value=True):
            sel = cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.backend, "codex")
        self.assertEqual(sel.label, "")
        # Codex doesn't get a lease
        self.assertEqual(accounts.list_leases(), [])

    def test_release_existing_claude_lease_when_picking_codex(self):
        cfg = _config("auto", prefer="claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # First call: claude OK, lease lean-fro
        with mock.patch.object(accounts, "probe_account",
                                return_value="opus"), \
             mock.patch.object(accounts, "probe_codex",
                                return_value=True):
            cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertEqual(state.account_label, "lean-fro")
        # Second call: claude exhausted, codex available — should release.
        with mock.patch.object(accounts, "probe_account",
                                return_value=None), \
             mock.patch.object(accounts, "probe_codex",
                                return_value=True):
            sel = cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.backend, "codex")
        self.assertEqual(state.account_label, "")
        self.assertEqual(accounts.list_leases(), [])


# --- Atomic revalidation ---------------------------------------------------


class RevalidateTests(_Harness):
    def test_quota_dropped_between_probe_and_acquire_skips(self):
        """Bulk probe says opus, but post-lease force probe says
        sonnet — should release and move on (only one account here, so
        end result is None).
        """
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # First probe (non-force) returns "opus", second probe (force=True
        # for revalidation) returns "sonnet" — quota dropped.
        calls = {"count": 0}
        def fake(cmd, label, force=False):
            calls["count"] += 1
            return "sonnet" if force else "opus"
        with mock.patch.object(accounts, "probe_account", side_effect=fake):
            sel = cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertIsNone(sel)
        # And the lease should have been released.
        self.assertEqual(accounts.list_leases(), [])


# --- Sticky leases ---------------------------------------------------------


class StickyLeaseTests(_Harness):
    def test_keeps_existing_lease_when_still_satisfies(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # First acquire
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        # Second acquire — same iteration → keep the lease, don't enter
        # the meta-lock (we shouldn't see additional lease writes).
        with mock.patch.object(accounts, "probe_account", return_value="opus") as m:
            sel = cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "lean-fro")
        # Only one probe call: the "is current lease still good?" check.
        self.assertEqual(m.call_count, 1)

    def test_releases_stale_lease_and_re_picks(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        self._write_credentials(5, "gamma")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # First acquire on lean-fro
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertEqual(state.account_label, "lean-fro")
        # Now lean-fro is exhausted but gamma still has opus.
        def fake(cmd, label, force=False):
            if label == "lean-fro":
                return None
            return "opus"
        with mock.patch.object(accounts, "probe_account", side_effect=fake):
            sel = cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "gamma")
        self.assertEqual(state.account_label, "gamma")


# --- Resume pinning --------------------------------------------------------


class ResumePinTests(_Harness):
    def test_pin_label_restricts_enumeration(self):
        cfg = _config("auto", prefer="claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        self._write_credentials(3, "beta")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=cdir,
                pin_backend="claude", pin_label="beta")
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "beta")

    def test_pin_label_unavailable_returns_none(self):
        cfg = _config("auto", prefer="claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(2, "alpha")
        # Note: 'beta' is the pin target but no credentials file for it.
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=cdir,
                pin_backend="claude", pin_label="beta")
        self.assertIsNone(sel)


# --- Force / FORCE_QUOTA_FILE bypass ---------------------------------------


class ForceTests(_Harness):
    def test_force_skips_quota_probe(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        # No probe stub: anything that probes would return None and fail.
        with mock.patch.object(accounts, "probe_account") as m:
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=cdir, force=True)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "lean-fro")
        self.assertEqual(m.call_count, 0)

    def test_force_quota_file_acts_as_force(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        cli.FORCE_QUOTA_FILE.write_text("1")
        try:
            state = self._make_state()
            cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
            accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
            with mock.patch.object(accounts, "probe_account") as m:
                sel = cli.acquire_backend(
                    cfg, state=state, claude_config_dir=cdir)
            self.assertIsNotNone(sel)
            self.assertEqual(m.call_count, 0)
        finally:
            cli.FORCE_QUOTA_FILE.unlink(missing_ok=True)


# --- Release helper --------------------------------------------------------


class ReleaseTests(_Harness):
    def test_release_clears_state_and_lease(self):
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro")
        state = self._make_state()
        cdir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            cli.acquire_backend(cfg, state=state, claude_config_dir=cdir)
        cli._release_account_lease(state, cdir)
        self.assertEqual(state.account_label, "")
        self.assertEqual(state.lease_acquired_at, 0.0)
        self.assertEqual(accounts.list_leases(), [])

    def test_release_is_noop_when_no_lease(self):
        state = self._make_state()
        cli._release_account_lease(state, None)
        self.assertEqual(state.account_label, "")


# --- Auth preflight: don't hand out a logged-out / expired account --------


class AuthPreflightTests(_Harness):
    def test_expired_credential_account_is_skipped(self):
        # Quota reads fine, but the OAuth token expired in the past:
        # the account is effectively logged out and every session on it
        # would fail auth at startup. acquire_backend must not lease it.
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "lean-fro", expires="2000-01-01T00:00:00Z")
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNone(sel)
        self.assertEqual(accounts.list_leases(), [])
        self.assertEqual(state.account_label, "")

    def test_healthy_account_chosen_over_expired_one(self):
        # An expired account is skipped and the next, valid account wins.
        cfg = _config("claude", accepted_models=["opus"])
        self._set_cfg(cfg)
        self._write_credentials(4, "expired-acct", expires="2000-01-01T00:00:00Z")
        self._write_credentials(5, "good-acct", expires="2030-01-01T00:00:00Z")
        state = self._make_state()
        config_dir = accounts.agent_claude_config_dir(cli.POD_DIR, state.short_id)
        accounts.ensure_agent_claude_config_dir(cli.POD_DIR, state.short_id)
        with mock.patch.object(accounts, "probe_account", return_value="opus"):
            sel = cli.acquire_backend(
                cfg, state=state, claude_config_dir=config_dir)
        self.assertIsNotNone(sel)
        self.assertEqual(sel.label, "good-acct")
        self.assertEqual([l.label for l in accounts.list_leases()], ["good-acct"])


if __name__ == "__main__":
    unittest.main()
