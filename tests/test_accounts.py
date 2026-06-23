"""Tests for pod.accounts — lease layer, candidate enumeration, and
credential mirror/harvest freshness logic."""

from __future__ import annotations

import json
import os
import subprocess
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import accounts


# --- Test helpers -----------------------------------------------------------


def _make_creds(label: str, expires_iso: str,
                 access_token: str = "sk-ant-test-AAAAAAAAAAAAAAAAAAAA") -> str:
    return json.dumps({
        "accountLabel": label,
        "claudeAiOauth": {
            "accessToken": access_token,
            "expiresAt": expires_iso,
            "refreshToken": "refresh-token-test",
        },
    })


class _IsolatedHome(unittest.TestCase):
    """Base class that redirects accounts.CLAUDE_DIR / LEASE_DIR to a tmp."""

    def setUp(self) -> None:
        import tempfile
        self.tmp = Path(tempfile.mkdtemp(prefix="pod-accounts-test-"))
        self._orig_claude_dir = accounts.CLAUDE_DIR
        self._orig_lease_dir = accounts.LEASE_DIR
        self._orig_lease_meta = accounts.LEASE_META_LOCK
        accounts.CLAUDE_DIR = self.tmp
        accounts.LEASE_DIR = self.tmp / "pod-account-leases"
        accounts.LEASE_META_LOCK = accounts.LEASE_DIR / ".lock"

    def tearDown(self) -> None:
        import shutil
        accounts.CLAUDE_DIR = self._orig_claude_dir
        accounts.LEASE_DIR = self._orig_lease_dir
        accounts.LEASE_META_LOCK = self._orig_lease_meta
        shutil.rmtree(self.tmp, ignore_errors=True)


# --- list_claude_accounts ---------------------------------------------------


class ListClaudeAccountsTests(_IsolatedHome):
    def test_returns_sorted_accounts_skipping_missing_labels(self):
        (self.tmp / "credentials2.json").write_text(_make_creds("alpha", "2026-12-31T00:00:00Z"))
        (self.tmp / "credentials4.json").write_text(_make_creds("gamma", "2026-12-31T00:00:00Z"))
        # No accountLabel → skipped
        (self.tmp / "credentials3.json").write_text(json.dumps({
            "claudeAiOauth": {"accessToken": "x", "expiresAt": "2026-12-31T00:00:00Z"}
        }))
        # Garbage JSON → skipped
        (self.tmp / "credentials5.json").write_text("{ not json")
        # Non-numeric suffix → skipped
        (self.tmp / "credentialsZ.json").write_text(_make_creds("ignored", "2026-12-31T00:00:00Z"))

        accts = accounts.list_claude_accounts()
        labels = [(a.number, a.label) for a in accts]
        self.assertEqual(labels, [(2, "alpha"), (4, "gamma")])

    def test_empty_directory_returns_empty_list(self):
        self.assertEqual(accounts.list_claude_accounts(), [])

    def test_current_account_marker_pins_single_account(self):
        # An external swap-account script marks account 4 active; pod must
        # defer to it and ignore the rest of the pool (the bug: pod used to
        # enumerate all and pick by its own order, overriding the choice).
        (self.tmp / "credentials2.json").write_text(_make_creds("alpha", "2026-12-31T00:00:00Z"))
        (self.tmp / "credentials3.json").write_text(_make_creds("beta", "2026-12-31T00:00:00Z"))
        (self.tmp / "credentials4.json").write_text(_make_creds("gamma", "2026-12-31T00:00:00Z"))
        (self.tmp / ".current-account").write_text("4\n")

        accts = accounts.list_claude_accounts()
        self.assertEqual([(a.number, a.label) for a in accts], [(4, "gamma")])

    def test_current_account_marker_unresolvable_falls_back_to_full_list(self):
        # Marker points at an account that isn't loadable → don't return
        # nothing; fall back to the full enumeration.
        (self.tmp / "credentials2.json").write_text(_make_creds("alpha", "2026-12-31T00:00:00Z"))
        (self.tmp / "credentials4.json").write_text(_make_creds("gamma", "2026-12-31T00:00:00Z"))
        (self.tmp / ".current-account").write_text("9\n")

        accts = accounts.list_claude_accounts()
        self.assertEqual([(a.number, a.label) for a in accts], [(2, "alpha"), (4, "gamma")])

    def test_current_account_marker_unparseable_falls_back(self):
        (self.tmp / "credentials2.json").write_text(_make_creds("alpha", "2026-12-31T00:00:00Z"))
        (self.tmp / ".current-account").write_text("not-a-number")

        accts = accounts.list_claude_accounts()
        self.assertEqual([(a.number, a.label) for a in accts], [(2, "alpha")])


# --- Keychain service name --------------------------------------------------


class KeychainServiceTests(unittest.TestCase):
    def test_default_dir(self):
        self.assertEqual(
            accounts.claude_keychain_service(None),
            "Claude Code-credentials")

    def test_nondefault_dir_hashes(self):
        s = accounts.claude_keychain_service(Path("/tmp/agent-foo"))
        self.assertTrue(s.startswith("Claude Code-credentials-"))
        self.assertEqual(len(s.split("-")[-1]), 8)

    def test_nfc_normalisation_stable(self):
        # Decomposed vs composed unicode should produce the same hash.
        nfc = Path("/tmp/café")  # composed
        nfd = Path("/tmp/café")  # decomposed
        self.assertEqual(
            accounts.claude_keychain_service(nfc),
            accounts.claude_keychain_service(nfd),
        )


# --- Lease layer ------------------------------------------------------------


class LeaseTests(_IsolatedHome):
    def test_acquire_release_roundtrip(self):
        with accounts.lease_critical_section():
            self.assertTrue(accounts.try_acquire_lease("lean-fro", "abcd1234"))
        leases = accounts.list_leases()
        self.assertEqual(len(leases), 1)
        self.assertEqual(leases[0].label, "lean-fro")
        self.assertEqual(leases[0].short_id, "abcd1234")
        self.assertTrue(accounts.release_lease("lean-fro", "abcd1234"))
        self.assertEqual(accounts.list_leases(), [])

    def test_second_agent_cannot_acquire_held_lease(self):
        with accounts.lease_critical_section():
            self.assertTrue(accounts.try_acquire_lease("qim", "aaaa"))
            self.assertFalse(accounts.try_acquire_lease("qim", "bbbb"))

    def test_same_agent_reacquire_is_noop_true(self):
        with accounts.lease_critical_section():
            self.assertTrue(accounts.try_acquire_lease("qim", "aaaa"))
            self.assertTrue(accounts.try_acquire_lease("qim", "aaaa"))

    def test_release_wrong_owner_refused(self):
        with accounts.lease_critical_section():
            self.assertTrue(accounts.try_acquire_lease("lean-fro", "aaaa"))
        self.assertFalse(accounts.release_lease("lean-fro", "bbbb"))
        self.assertEqual(len(accounts.list_leases()), 1)

    def test_release_missing_is_false(self):
        self.assertFalse(accounts.release_lease("nobody", "ZZZZ"))

    def test_evict_orphans(self):
        with accounts.lease_critical_section():
            accounts.try_acquire_lease("alpha", "live1")
            accounts.try_acquire_lease("beta", "dead1")
            accounts.try_acquire_lease("gamma", "live2")
            evicted = accounts.evict_orphan_leases(["live1", "live2"])
        self.assertEqual(sorted(evicted), ["beta"])
        remaining = sorted(l.label for l in accounts.list_leases())
        self.assertEqual(remaining, ["alpha", "gamma"])

    def test_label_with_unsafe_chars_does_not_escape_dir(self):
        # Account labels shouldn't normally contain /, but the lease
        # filename builder must defend against it.
        with accounts.lease_critical_section():
            self.assertTrue(accounts.try_acquire_lease("a/b/../c", "abcd"))
        leases = accounts.list_leases()
        self.assertEqual(len(leases), 1)
        # The on-disk path must remain inside LEASE_DIR.
        for entry in accounts.LEASE_DIR.iterdir():
            self.assertEqual(entry.parent, accounts.LEASE_DIR)


# --- expiresAt parsing ------------------------------------------------------


class ExpiresAtTests(unittest.TestCase):
    def test_iso_string(self):
        blob = _make_creds("x", "2026-12-31T00:00:00Z")
        self.assertGreater(accounts._expires_at(blob), 1_700_000_000)

    def test_epoch_ms(self):
        ms = 1_900_000_000_000  # year ~2030 in ms
        blob = json.dumps({
            "accountLabel": "x",
            "claudeAiOauth": {"accessToken": "y", "expiresAt": ms},
        })
        secs = accounts._expires_at(blob)
        self.assertAlmostEqual(secs, ms / 1000.0, places=3)

    def test_epoch_seconds(self):
        s = 1_900_000_000  # year ~2030 in s
        blob = json.dumps({
            "accountLabel": "x",
            "claudeAiOauth": {"accessToken": "y", "expiresAt": s},
        })
        self.assertAlmostEqual(accounts._expires_at(blob), float(s), places=3)

    def test_missing_or_bad(self):
        self.assertEqual(accounts._expires_at(None), 0.0)
        self.assertEqual(accounts._expires_at("{}"), 0.0)
        self.assertEqual(accounts._expires_at("{ bogus"), 0.0)


# --- Mirror / harvest -------------------------------------------------------


class MirrorHarvestTests(_IsolatedHome):
    """These tests stub out the keychain so they run on Linux CI too."""

    def setUp(self) -> None:
        super().setUp()
        self._kc: dict[str, str] = {}

        def fake_read(service: str) -> str | None:
            return self._kc.get(service)

        def fake_write(service: str, blob: str) -> bool:
            self._kc[service] = blob
            return True

        self._kc_read = mock.patch.object(accounts, "_keychain_read", side_effect=fake_read)
        self._kc_write = mock.patch.object(accounts, "_keychain_write", side_effect=fake_write)
        self._kc_read.start()
        self._kc_write.start()
        self.addCleanup(self._kc_read.stop)
        self.addCleanup(self._kc_write.stop)

    def _write_canonical(self, num: int, label: str, expires: str) -> Path:
        path = self.tmp / f"credentials{num}.json"
        path.write_text(_make_creds(label, expires))
        return path

    def test_mirror_writes_when_isolated_empty(self):
        self._write_canonical(4, "lean-fro", "2026-12-31T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        wrote = accounts.mirror_canonical_to_isolated(
            "lean-fro", 4, config_dir)
        self.assertTrue(wrote)
        service = accounts.claude_keychain_service(config_dir)
        self.assertIn(service, self._kc)
        self.assertTrue((config_dir / ".credentials.json").exists())

    def test_mirror_skips_when_isolated_is_fresher(self):
        # Canonical expires 2026; isolated already has 2027 → don't clobber.
        self._write_canonical(4, "lean-fro", "2026-12-31T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        self._kc[service] = _make_creds("lean-fro", "2027-12-31T00:00:00Z")
        wrote = accounts.mirror_canonical_to_isolated(
            "lean-fro", 4, config_dir)
        self.assertFalse(wrote)
        # The fresher blob is still there.
        self.assertIn("2027", self._kc[service])

    def test_harvest_writes_when_isolated_is_fresher(self):
        path = self._write_canonical(4, "lean-fro", "2026-12-31T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        # Simulate a mid-session refresh: isolated now has a 2027 expiry.
        self._kc[service] = _make_creds(
            "lean-fro", "2027-12-31T00:00:00Z",
            access_token="sk-ant-test-NEWNEWNEWNEWNEWNEWNE")
        wrote = accounts.harvest_isolated_to_canonical(
            "lean-fro", 4, config_dir)
        self.assertTrue(wrote)
        # Canonical now has 2027.
        canonical_after = path.read_text()
        self.assertIn("2027", canonical_after)
        self.assertIn("NEWNEWNEWNEWNEWNEW", canonical_after)

    def test_harvest_skips_when_canonical_is_fresher_or_equal(self):
        self._write_canonical(4, "lean-fro", "2027-12-31T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        self._kc[service] = _make_creds("lean-fro", "2026-12-31T00:00:00Z")
        wrote = accounts.harvest_isolated_to_canonical(
            "lean-fro", 4, config_dir)
        self.assertFalse(wrote)

    def test_harvest_refuses_non_json_isolated(self):
        path = self._write_canonical(4, "lean-fro", "2026-01-01T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        self._kc[service] = "not even json"
        wrote = accounts.harvest_isolated_to_canonical(
            "lean-fro", 4, config_dir)
        self.assertFalse(wrote)
        # Canonical untouched.
        self.assertIn("2026", path.read_text())

    def test_mirror_clears_stale_when_keychain_write_fails(self):
        # Stale isolated entry (older expiry) + write failure → we delete
        # the stale entry so Claude Code falls through to the file we
        # just wrote. mirror returns True; no exception.
        self._write_canonical(4, "lean-fro", "2027-01-01T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        self._kc[service] = _make_creds(
            "qim", "2026-01-01T00:00:00Z")  # wrong account, older
        # Override write to fail; delete uses the fake_kc dict so
        # patching it is enough to simulate "delete works".
        self._kc_write.stop()
        with mock.patch.object(accounts, "_keychain_write", return_value=False), \
             mock.patch.object(accounts, "_keychain_delete",
                                side_effect=lambda s: self._kc.pop(s, None)):
            wrote = accounts.mirror_canonical_to_isolated(
                "lean-fro", 4, config_dir)
        self.assertTrue(wrote)
        self.assertNotIn(service, self._kc)
        # File fallback exists with the right account.
        self.assertIn("lean-fro",
                      (config_dir / ".credentials.json").read_text())

    def test_mirror_raises_when_stale_entry_persists(self):
        # Write fails AND delete is a no-op → stale entry persists →
        # raise so the caller releases the lease and tries another
        # candidate instead of launching the agent under qim's token
        # while believing it's lean-fro.
        self._write_canonical(4, "lean-fro", "2027-01-01T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        service = accounts.claude_keychain_service(config_dir)
        self._kc[service] = _make_creds("qim", "2026-01-01T00:00:00Z")
        self._kc_write.stop()
        with mock.patch.object(accounts, "_keychain_write", return_value=False), \
             mock.patch.object(accounts, "_keychain_delete",
                                side_effect=lambda s: None):
            with self.assertRaises(accounts.CredentialMirrorError):
                accounts.mirror_canonical_to_isolated(
                    "lean-fro", 4, config_dir)

    # --- preflight_and_mirror: validate + mirror as one locked op ----------

    def test_preflight_ok_mirrors_valid_account(self):
        self._write_canonical(4, "lean-fro", "2030-12-31T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        status = accounts.preflight_and_mirror(
            "lean-fro", 4, config_dir, now=0.0, skew=60)
        self.assertEqual(status, "ok")
        self.assertTrue((config_dir / ".credentials.json").exists())

    def test_preflight_missing_credential_is_not_mirrored(self):
        # No credentials4.json on disk → account logged out.
        config_dir = self.tmp / "claude-config" / "abcd1234"
        status = accounts.preflight_and_mirror(
            "lean-fro", 4, config_dir, now=0.0, skew=60)
        self.assertEqual(status, "missing")
        self.assertFalse((config_dir / ".credentials.json").exists())

    def test_preflight_expired_token_is_not_mirrored(self):
        self._write_canonical(4, "lean-fro", "2000-01-01T00:00:00Z")
        config_dir = self.tmp / "claude-config" / "abcd1234"
        status = accounts.preflight_and_mirror(
            "lean-fro", 4, config_dir, now=2.0e9, skew=60)  # now ≫ 2000 expiry
        self.assertEqual(status, "expired")
        self.assertFalse((config_dir / ".credentials.json").exists())

    def test_preflight_unknown_expiry_proceeds(self):
        # Credential present but no parseable expiresAt → not falsely
        # quarantined; treated as ok and mirrored.
        (self.tmp / "credentials4.json").write_text(json.dumps({
            "accountLabel": "lean-fro",
            "claudeAiOauth": {
                "accessToken": "sk-ant-test-AAAAAAAAAAAAAAAAAAAA"},
        }))
        config_dir = self.tmp / "claude-config" / "abcd1234"
        status = accounts.preflight_and_mirror(
            "lean-fro", 4, config_dir, now=2.0e9, skew=60)
        self.assertEqual(status, "ok")
        self.assertTrue((config_dir / ".credentials.json").exists())


# --- probe_account / probe_codex --------------------------------------------


class ProbeTests(unittest.TestCase):
    def test_probe_account_returns_stdout_on_success(self):
        cp = subprocess.CompletedProcess(args=[], returncode=0,
                                          stdout="opus\n", stderr="")
        with mock.patch("subprocess.run", return_value=cp) as m:
            out = accounts.probe_account(
                "/bin/quota-helper", "lean-fro")
        self.assertEqual(out, "opus")
        args, _ = m.call_args
        self.assertEqual(args[0], ["/bin/quota-helper", "--account", "lean-fro"])

    def test_probe_account_force_flag(self):
        cp = subprocess.CompletedProcess(args=[], returncode=0,
                                          stdout="sonnet", stderr="")
        with mock.patch("subprocess.run", return_value=cp) as m:
            accounts.probe_account("/bin/quota-helper", "qim", force=True)
        args, _ = m.call_args
        self.assertIn("--force", args[0])

    def test_probe_account_nonzero_exit_returns_none(self):
        cp = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="")
        with mock.patch("subprocess.run", return_value=cp):
            self.assertIsNone(accounts.probe_account(
                "/bin/q", "exhausted"))

    def test_probe_account_oserror_returns_none(self):
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            self.assertIsNone(accounts.probe_account("/bin/missing", "x"))

    def test_probe_codex_success(self):
        cp = subprocess.CompletedProcess(args=[], returncode=0,
                                          stdout="gpt-5.5\n", stderr="")
        with mock.patch("subprocess.run", return_value=cp):
            self.assertTrue(accounts.probe_codex("/bin/codex-q"))

    def test_probe_codex_cache_stale_exit2(self):
        cp = subprocess.CompletedProcess(args=[], returncode=2,
                                          stdout="", stderr="")
        with mock.patch("subprocess.run", return_value=cp):
            self.assertFalse(accounts.probe_codex("/bin/codex-q"))


# --- Candidate enumeration --------------------------------------------------


class EnumerateCandidatesTests(unittest.TestCase):
    def _accounts(self, *labels: str) -> list[accounts.Account]:
        return [accounts.Account(label=l, number=i + 1, path=Path(f"/dev/null/{l}"))
                for i, l in enumerate(labels)]

    def test_opus_account_yields_opus_candidate(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a", "b"),
            available_by_label={"a": "opus", "b": None},
            accepted_models=["opus"],
            codex_available=False,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        self.assertEqual([(c.backend, c.label, c.model) for c in cands],
                         [("claude", "a", "opus")])

    def test_sonnet_only_account_skipped_when_opus_required(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("only-sonnet"),
            available_by_label={"only-sonnet": "sonnet"},
            accepted_models=["opus"],
            codex_available=False,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        self.assertEqual(cands, [])

    def test_sonnet_accepted_after_opus(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("opus-acct", "sonnet-acct"),
            available_by_label={"opus-acct": "opus", "sonnet-acct": "sonnet"},
            accepted_models=["opus", "sonnet"],
            codex_available=False,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        # First-satisfiable per account: opus-acct -> opus, sonnet-acct -> sonnet
        self.assertEqual([(c.label, c.model) for c in cands],
                         [("opus-acct", "opus"), ("sonnet-acct", "sonnet")])

    def test_one_candidate_per_account_not_one_per_tier(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a"),
            available_by_label={"a": "opus"},
            accepted_models=["opus", "sonnet"],
            codex_available=False,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        # Opus satisfies both tiers but we only emit one candidate per account.
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0].model, "opus")

    def test_codex_added_after_claude_when_prefer_claude(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a"),
            available_by_label={"a": "opus"},
            accepted_models=["opus"],
            codex_available=True,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        self.assertEqual([(c.backend, c.label) for c in cands],
                         [("claude", "a"), ("codex", "")])

    def test_codex_first_when_prefer_codex(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a"),
            available_by_label={"a": "opus"},
            accepted_models=["opus"],
            codex_available=True,
            codex_model="gpt-5.5",
            prefer="codex",
        )
        self.assertEqual([(c.backend, c.label) for c in cands],
                         [("codex", ""), ("claude", "a")])

    def test_no_candidates_when_all_exhausted(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a", "b"),
            available_by_label={"a": None, "b": None},
            accepted_models=["opus", "sonnet"],
            codex_available=False,
            codex_model="gpt-5.5",
            prefer="claude",
        )
        self.assertEqual(cands, [])

    def test_pin_label_restricts_to_that_account(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a", "b"),
            available_by_label={"a": "opus", "b": "opus"},
            accepted_models=["opus"],
            codex_available=True,
            codex_model="gpt-5.5",
            prefer="claude",
            pin_label="b",
        )
        # Only b, no codex (pinning is for resume — must be the same backend too).
        self.assertEqual([(c.backend, c.label) for c in cands],
                         [("claude", "b")])

    def test_pin_label_with_missing_account_yields_empty(self):
        cands = accounts.enumerate_candidates(
            claude_accounts=self._accounts("a"),
            available_by_label={"a": "opus"},
            accepted_models=["opus"],
            codex_available=True,
            codex_model="gpt-5.5",
            prefer="claude",
            pin_label="nonexistent",
        )
        self.assertEqual(cands, [])


# --- Per-agent dir helpers --------------------------------------------------


class AgentConfigDirTests(unittest.TestCase):
    def test_returns_per_agent_subdir(self):
        d = accounts.agent_claude_config_dir(Path("/tmp/.pod"), "abcd1234")
        self.assertEqual(d, Path("/tmp/.pod/claude-config/abcd1234"))

    def test_ensure_creates_minimal_layout(self):
        import tempfile
        tmp = Path(tempfile.mkdtemp(prefix="pod-test-"))
        try:
            d = accounts.ensure_agent_claude_config_dir(tmp / ".pod", "zzzzzzzz")
            self.assertTrue(d.exists())
            self.assertTrue((d / "settings.json").exists())
            self.assertTrue((d / "projects").is_dir())
            # No credentials yet — that's mirror_canonical_to_isolated's job.
            self.assertFalse((d / ".credentials.json").exists())
        finally:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
