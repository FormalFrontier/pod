"""Tests for the idempotent `_release_claim` REST probe and the
`_GraphQLRateLimited` sentinel propagation through the release loops.

These cover the regression that caused 777+ "Failed to remove claimed
label" log lines on a single hex pod over a week: every release attempt
on an already-closed/already-unlabelled issue would fail, get re-queued
into claim-history, and burn ~3 GraphQL calls each retry × N agents ×
every 10 min.
"""
import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


def _result(stdout: str = "", stderr: str = "", returncode: int = 0):
    """Build a fake subprocess.run result."""
    r = mock.Mock()
    r.stdout = stdout
    r.stderr = stderr
    r.returncode = returncode
    return r


class IsRateLimitErrorTests(unittest.TestCase):
    def test_detects_text_stderr(self):
        self.assertTrue(cli._is_rate_limit_error(
            _result(stderr="GraphQL: API rate limit already exceeded for user ID 1")))

    def test_detects_bytes_stderr(self):
        self.assertTrue(cli._is_rate_limit_error(
            _result(stderr=b"API rate limit exceeded")))

    def test_detects_secondary_limit(self):
        self.assertTrue(cli._is_rate_limit_error(
            _result(stderr="You have triggered an abuse detection mechanism. "
                    "Secondary rate limit hit.")))

    def test_detects_in_stdout(self):
        # Some gh failure paths route the error to stdout.
        self.assertTrue(cli._is_rate_limit_error(
            _result(stdout="rate limit", stderr="")))

    def test_negative_normal_error(self):
        self.assertFalse(cli._is_rate_limit_error(
            _result(stderr="HTTP 404: Not Found")))

    def test_negative_empty(self):
        self.assertFalse(cli._is_rate_limit_error(_result()))


class ReleaseClaimIdempotenceTests(unittest.TestCase):
    """REST probe must short-circuit before any GraphQL call when the claim
    is already terminal."""

    def setUp(self):
        self._patch_repo = mock.patch.object(
            cli, "_get_repo", return_value="acme/widgets")
        self._patch_repo.start()

    def tearDown(self):
        self._patch_repo.stop()

    def test_closed_issue_no_label_returns_true_without_graphql(self):
        """The most common stale-history case: issue closed long ago, label
        already removed. Pre-fix this triggered the 777-failure loop."""
        calls: list[list[str]] = []

        def fake_run(argv, **kwargs):
            calls.append(list(argv))
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "closed", "labels": ["agent-plan"]}\n')
            raise AssertionError(f"unexpected gh call: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertTrue(cli._release_claim("42", "uuid-x", restart_count=0))
        # Only the REST probe ran — no `gh issue view` (CAS), no
        # `gh issue edit`, no `gh issue comment`.
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["gh", "api"])
        self.assertNotIn("--remove-label", " ".join(calls[0]))

    def test_open_issue_no_label_returns_true_without_graphql(self):
        """Concurrent reconciler removed the label; we must not re-edit."""
        calls: list[list[str]] = []

        def fake_run(argv, **kwargs):
            calls.append(list(argv))
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["agent-plan"]}\n')
            raise AssertionError(f"unexpected gh call: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertTrue(cli._release_claim("99", "uuid-x", restart_count=0))
        self.assertEqual(len(calls), 1)

    def test_closed_with_label_returns_true_leaves_label_alone(self):
        """Closed issue still carrying `claimed` is the quota-safe terminal
        case — accept the stale label rather than spend GraphQL cleaning it."""
        calls: list[list[str]] = []

        def fake_run(argv, **kwargs):
            calls.append(list(argv))
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "closed", "labels": ["claimed", "agent-plan"]}\n')
            raise AssertionError(f"unexpected gh call: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertTrue(cli._release_claim("100", "uuid-x", restart_count=0))
        self.assertEqual(len(calls), 1)

    def test_open_with_label_runs_full_path(self):
        """Real release: REST probe → CAS → edit → comment, all succeed."""
        calls: list[list[str]] = []

        def fake_run(argv, **kwargs):
            calls.append(list(argv))
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["claimed"]}\n')
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(stdout='"Claimed by session `uuid-x` on branch `agent/abcd`"\n')
            if argv[:3] == ["gh", "issue", "edit"]:
                return _result(returncode=0)
            if argv[:3] == ["gh", "issue", "comment"]:
                return _result(returncode=0)
            raise AssertionError(f"unexpected gh call: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertTrue(cli._release_claim("123", "uuid-x", restart_count=0))
        verbs = [c[1] if c[1] == "api" else f"{c[1]} {c[2]}" for c in calls]
        self.assertEqual(verbs, ["api", "issue view", "issue edit", "issue comment"])

    def test_probe_failure_falls_through_to_legacy_path(self):
        """If the REST probe itself errors, behave like the old code:
        proceed to the CAS + edit path."""
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(returncode=1, stderr="probe failed")
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(stdout='"Claimed by session `uuid-x`"\n')
            if argv[:3] == ["gh", "issue", "edit"]:
                return _result(returncode=0)
            if argv[:3] == ["gh", "issue", "comment"]:
                return _result(returncode=0)
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertTrue(cli._release_claim("7", "uuid-x", restart_count=0))


class ReleaseClaimSentinelTests(unittest.TestCase):
    """The `_GraphQLRateLimited` sentinel must propagate, not be silently
    converted to `return False` by any catch in `_release_claim`."""

    def setUp(self):
        self._patch_repo = mock.patch.object(
            cli, "_get_repo", return_value="acme/widgets")
        self._patch_repo.start()

    def tearDown(self):
        self._patch_repo.stop()

    def test_sentinel_raised_on_rest_probe_rate_limit(self):
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(returncode=1, stderr="API rate limit exceeded")
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(cli._GraphQLRateLimited):
                cli._release_claim("42", "uuid-x", restart_count=0)

    def test_sentinel_raised_on_cas_rate_limit(self):
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["claimed"]}\n')
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(returncode=1, stderr="GraphQL: API rate limit exceeded")
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(cli._GraphQLRateLimited):
                cli._release_claim("42", "uuid-x", restart_count=0)

    def test_sentinel_raised_on_edit_rate_limit(self):
        """Regression test for the critical bug Codex flagged: the original
        `except Exception: return False` would have swallowed this."""
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["claimed"]}\n')
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(stdout='"Claimed by session `uuid-x`"\n')
            if argv[:3] == ["gh", "issue", "edit"]:
                return _result(returncode=1, stderr="GraphQL: API rate limit already exceeded for user ID 1")
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(cli._GraphQLRateLimited):
                cli._release_claim("42", "uuid-x", restart_count=0)

    def test_sentinel_raised_on_comment_rate_limit(self):
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["claimed"]}\n')
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(stdout='"Claimed by session `uuid-x`"\n')
            if argv[:3] == ["gh", "issue", "edit"]:
                return _result(returncode=0)
            if argv[:3] == ["gh", "issue", "comment"]:
                return _result(returncode=1, stderr="rate limit")
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(cli._GraphQLRateLimited):
                cli._release_claim("42", "uuid-x", restart_count=0)

    def test_normal_edit_failure_returns_false_not_sentinel(self):
        """Non-rate-limit failures still return False so the caller can
        re-queue. The sentinel is reserved for actual quota exhaustion."""
        def fake_run(argv, **kwargs):
            if argv[:2] == ["gh", "api"]:
                return _result(stdout='{"state": "open", "labels": ["claimed"]}\n')
            if argv[:3] == ["gh", "issue", "view"]:
                return _result(stdout='"Claimed by session `uuid-x`"\n')
            if argv[:3] == ["gh", "issue", "edit"]:
                return _result(returncode=1, stderr=b"HTTP 404")
            raise AssertionError(f"unexpected: {argv}")

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            self.assertFalse(cli._release_claim("42", "uuid-x", restart_count=0))


class HousekeepingFilelockTests(unittest.TestCase):
    """The global housekeeping lock must serialise across processes/threads
    so N agents don't each fire their own sweep per window."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._lock = Path(self._tmp.name) / "housekeeping.lock"
        self._stamp = Path(self._tmp.name) / "housekeeping.stamp"
        self._patch_lock = mock.patch.object(
            cli, "HOUSEKEEPING_LOCK_PATH", self._lock)
        self._patch_stamp = mock.patch.object(
            cli, "HOUSEKEEPING_STAMP_PATH", self._stamp)
        self._patch_lock.start()
        self._patch_stamp.start()

    def tearDown(self):
        self._patch_lock.stop()
        self._patch_stamp.stop()
        self._tmp.cleanup()

    def test_concurrent_acquire_only_one_succeeds(self):
        """Two threads racing to enter the housekeeping sweep — exactly
        one acquires the lock, the other gets owns_sweep=False."""
        results: list[bool] = []
        results_lock = threading.Lock()
        # First holder waits long enough that the second thread definitely
        # contends for the lock.
        first_inside = threading.Event()
        release_first = threading.Event()

        def first():
            with cli._housekeeping_filelock() as owns:
                with results_lock:
                    results.append(owns)
                first_inside.set()
                release_first.wait(timeout=5.0)

        def second():
            first_inside.wait(timeout=5.0)
            with cli._housekeeping_filelock() as owns:
                with results_lock:
                    results.append(owns)

        t1 = threading.Thread(target=first)
        t2 = threading.Thread(target=second)
        t1.start()
        t2.start()
        # Give the second thread time to attempt acquisition
        time.sleep(0.2)
        release_first.set()
        t1.join(timeout=5.0)
        t2.join(timeout=5.0)
        self.assertEqual(sorted(results), [False, True])

    def test_due_when_no_stamp(self):
        self.assertTrue(cli._housekeeping_due(600))

    def test_not_due_when_stamp_fresh(self):
        cli._housekeeping_mark_done()
        self.assertFalse(cli._housekeeping_due(600))

    def test_due_when_stamp_old(self):
        self._stamp.write_text(f"{time.time() - 1000}\n")
        self.assertTrue(cli._housekeeping_due(600))

    def test_due_when_stamp_unparseable(self):
        self._stamp.write_text("not a number\n")
        self.assertTrue(cli._housekeeping_due(600))


if __name__ == "__main__":
    unittest.main()
