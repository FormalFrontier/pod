"""Tests for the Python coordination module (pod/coordination.py)."""

from __future__ import annotations

import contextlib
import io
import json
import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import coordination
from pod import github as gh

from _gh_helpers import fake_response, patch_client


def _make_ctx(repo: str = "owner/repo",
              session_id: str = "sess-A",
              branch: str = "agent/abcd",
              protected_files=("PLAN.md",)) -> coordination.CoordinationContext:
    return coordination.CoordinationContext(
        repo=repo,
        base_branch="main",
        session_id=session_id,
        branch=branch,
        protected_files=list(protected_files),
        stuck_ci_minutes=120,
        include_untrusted=False,
        repo_slug=repo.replace("/", "-"),
        git_toplevel="/tmp/fake",
        cache_key="-fake",
    )


@contextlib.contextmanager
def _capture_stdout():
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        yield buf


def _gh_result(stdout: str = "", stderr: str = "",
               returncode: int = 0) -> mock.Mock:
    r = mock.Mock()
    r.stdout = stdout
    r.stderr = stderr
    r.returncode = returncode
    return r


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class HelperTests(unittest.TestCase):
    def test_iso_to_epoch_round_trip(self):
        e = coordination._iso_to_epoch("2026-05-10T14:30:00Z")
        self.assertIsInstance(e, int)
        self.assertGreater(e, 0)

    def test_iso_to_epoch_bad_returns_none(self):
        self.assertIsNone(coordination._iso_to_epoch(""))
        self.assertIsNone(coordination._iso_to_epoch("not-a-date"))

    def test_safe_json_default(self):
        self.assertEqual(coordination._safe_json("", default=[]), [])
        self.assertEqual(coordination._safe_json("bad", default={}), {})
        self.assertEqual(coordination._safe_json('{"a":1}', default=None),
                         {"a": 1})

    def test_unclaimed_filter_excludes_claimed(self):
        items = [
            {"number": 1, "labels": [{"name": "agent-plan"}],
             "createdAt": "2026-05-09"},
            {"number": 2, "labels": [{"name": "claimed"}],
             "createdAt": "2026-05-08"},
            {"number": 3, "labels": [{"name": "agent-plan"},
                                       {"name": "critical-path"}],
             "createdAt": "2026-05-10"},
            {"number": 4, "labels": [{"name": "agent-plan"},
                                       {"name": "blocked"}]},
            {"number": 5, "labels": [{"name": "agent-plan"},
                                       {"name": "has-pr"}]},
            {"number": 6, "labels": [{"name": "agent-plan"},
                                       {"name": "replan"}]},
        ]
        out = coordination._unclaimed_filter(items)
        # Excluded: 2 (claimed), 4 (blocked), 5 (has-pr), 6 (replan).
        # Order: critical-path first (3), then by createdAt (1).
        self.assertEqual([x["number"] for x in out], [3, 1])


# ---------------------------------------------------------------------------
# Subcommand: claim
# ---------------------------------------------------------------------------

class ClaimTests(unittest.TestCase):
    def setUp(self):
        self._prov_ok = mock.patch.object(
            coordination, "_check_provenance", return_value=(True, ""))
        self._prov_ok.start()
        self.addCleanup(self._prov_ok.stop)

    def test_provenance_fail_returns_1(self):
        with mock.patch.object(coordination, "_check_provenance",
                                return_value=(False, "untrusted author")):
            with _capture_stdout() as buf, patch_client() as client:
                rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("CLAIM FAILED:", buf.getvalue())
        self.assertIn("untrusted author", buf.getvalue())
        # No gh_cli or HTTP calls should have happened.
        self.assertEqual(client.calls, [])

    def test_already_claimed_returns_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,claimed,critical-path")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(
                gh_cli_handler=handler):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("already claimed", buf.getvalue())

    def test_blocked_returns_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,blocked")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("blocked by dependencies", buf.getvalue())

    def test_replan_returns_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,replan")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("needs replan", buf.getvalue())

    def test_has_pr_returns_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,has-pr")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("already has an open PR", buf.getvalue())

    def test_clean_claim_succeeds(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan")
            return _gh_result()

        # Empty comments page (after the race-detect sleep) → no other
        # claimants visible → claim succeeds.
        empty_page = fake_response(200, body=[])
        with _capture_stdout() as buf, \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/42/comments"): empty_page,
             }, gh_cli_handler=handler), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 0)
        self.assertIn("Claimed issue #42", buf.getvalue())

    def test_race_recheck_failure_fails_closed(self):
        """If the post-sleep comment re-read returns non-OK, the bash
        original would have died (set -euo pipefail); the Python port
        must fail closed with a clear message rather than silently
        succeeding."""
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan")
            return _gh_result()

        # Race-check page errors out — must NOT succeed.
        error_page = fake_response(500, body={"message": "server error"})
        with _capture_stdout() as buf, \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/42/comments"): error_page,
             }, gh_cli_handler=handler), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_claim(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("CLAIM FAILED", buf.getvalue())
        self.assertIn("race verification", buf.getvalue())

    def test_race_lost_returns_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan")
            return _gh_result()

        # paginate sees TWO recent claim comments; the lowest-sorted body
        # belongs to a different session (alphabetically lower).
        now_iso = "2026-05-10T14:30:00Z"
        page = fake_response(200, body=[
            {"body": "Claimed by session `aaa-other` on branch `x`",
             "created_at": now_iso},
            {"body": "Claimed by session `sess-A` on branch `x`",
             "created_at": now_iso},
        ])
        with _capture_stdout() as buf, \
             mock.patch("pod.coordination._iso_to_epoch",
                        return_value=int(time.time())), \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/42/comments"): page,
             }, gh_cli_handler=handler), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_claim(_make_ctx(session_id="sess-A"), ["42"])
        self.assertEqual(rc, 1)
        self.assertIn("Race detected", buf.getvalue())
        self.assertIn("another agent won", buf.getvalue())

    def test_race_won_prints_won_message(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan")
            return _gh_result()

        page = fake_response(200, body=[
            {"body": "Claimed by session `sess-A` on branch `x`",
             "created_at": "2026-05-10T14:30:00Z"},
            {"body": "Claimed by session `zzz-loser` on branch `x`",
             "created_at": "2026-05-10T14:30:00Z"},
        ])
        with _capture_stdout() as buf, \
             mock.patch("pod.coordination._iso_to_epoch",
                        return_value=int(time.time())), \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/42/comments"): page,
             }, gh_cli_handler=handler), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_claim(_make_ctx(session_id="sess-A"), ["42"])
        self.assertEqual(rc, 0)
        self.assertIn("this session won", buf.getvalue())


# ---------------------------------------------------------------------------
# Subcommand: skip
# ---------------------------------------------------------------------------

class SkipTests(unittest.TestCase):
    def test_skip_emits_label_and_comment(self):
        calls: list[tuple] = []

        def handler(*argv):
            calls.append(argv)
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_skip(_make_ctx(), ["42", "stale plan"])
        self.assertEqual(rc, 0)
        self.assertIn("Skipped issue #42 (marked replan): stale plan",
                       buf.getvalue())
        verbs = [argv[:2] for argv in calls]
        self.assertIn(("issue", "edit"), verbs)
        self.assertIn(("issue", "comment"), verbs)


# ---------------------------------------------------------------------------
# Subcommand: queue-depth
# ---------------------------------------------------------------------------

class QueueDepthTests(unittest.TestCase):
    def test_outputs_integer(self):
        with mock.patch.object(coordination, "_unclaimed_issues",
                                return_value=[{"number": 1}, {"number": 2}]):
            with _capture_stdout() as buf:
                rc = coordination.cmd_queue_depth(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertEqual(buf.getvalue().strip(), "2")

    def test_zero_with_no_unclaimed(self):
        with mock.patch.object(coordination, "_unclaimed_issues",
                                return_value=[]):
            with _capture_stdout() as buf:
                rc = coordination.cmd_queue_depth(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertEqual(buf.getvalue().strip(), "0")

    def test_extra_label_forwarded(self):
        captured: list = []

        def fake(ctx, extra_label=None):
            captured.append(extra_label)
            return []

        with mock.patch.object(coordination, "_unclaimed_issues",
                                side_effect=fake):
            with _capture_stdout() as buf:
                coordination.cmd_queue_depth(_make_ctx(), ["feature"])
        self.assertEqual(captured, ["feature"])
        self.assertEqual(buf.getvalue().strip(), "0")


# ---------------------------------------------------------------------------
# Subcommand: set-target / set-min-queue
# ---------------------------------------------------------------------------

class PlannerAdvisoryFileTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.repo_root = Path(self._tmp.name)
        (self.repo_root / ".pod").mkdir()
        # Stub _main_repo_root() to return our tempdir.
        p = mock.patch.object(coordination, "_main_repo_root",
                               return_value=self.repo_root)
        p.start()
        self.addCleanup(p.stop)

    def test_set_target_writes_file(self):
        with _capture_stdout() as buf:
            rc = coordination.cmd_set_target(_make_ctx(), ["3"])
        self.assertEqual(rc, 0)
        self.assertIn("Planner target set to 3", buf.getvalue())
        content = (self.repo_root / ".pod" / "planner-target").read_text()
        self.assertIn("\n3\n", content)

    def test_set_min_queue_writes_file(self):
        with _capture_stdout() as buf:
            rc = coordination.cmd_set_min_queue(_make_ctx(), ["0"])
        self.assertEqual(rc, 0)
        self.assertIn("Planner min_queue set to 0", buf.getvalue())
        content = (self.repo_root / ".pod" / "planner-min-queue").read_text()
        self.assertIn("\n0\n", content)

    def test_no_pod_dir_returns_1(self):
        # Remove the .pod dir
        for p in (self.repo_root / ".pod").iterdir():
            p.unlink()
        (self.repo_root / ".pod").rmdir()
        with _capture_stdout(), self.assertLogs() if False else contextlib.nullcontext():
            rc = coordination.cmd_set_target(_make_ctx(), ["3"])
        self.assertEqual(rc, 1)


# ---------------------------------------------------------------------------
# Subcommand: return-to-human family
# ---------------------------------------------------------------------------

class ReturnToHumanTests(unittest.TestCase):
    def setUp(self):
        # Skip planner-lock resolution by pre-seeding the ctx.
        self.ctx = _make_ctx()
        self.ctx._planner_lock_issue = 999

    def test_signal_sets_label(self):
        calls: list[tuple] = []

        def handler(*argv):
            calls.append(argv)
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_return_to_human(self.ctx, [])
        self.assertEqual(rc, 0)
        self.assertIn("Return-to-human signal sent", buf.getvalue())
        self.assertIn("999", buf.getvalue())
        verbs = [argv[:2] for argv in calls]
        self.assertIn(("issue", "edit"), verbs)

    def test_check_true_exits_0(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,return-to-human")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_check_return_to_human(self.ctx, [])
        self.assertEqual(rc, 0)
        self.assertEqual(buf.getvalue().strip(), "true")

    def test_check_false_exits_1(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view"):
                return _gh_result(stdout="agent-plan,coordination")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_check_return_to_human(self.ctx, [])
        self.assertEqual(rc, 1)
        self.assertEqual(buf.getvalue().strip(), "false")

    def test_clear_removes_label(self):
        calls: list[tuple] = []

        def handler(*argv):
            calls.append(argv)
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_clear_return_to_human(self.ctx, [])
        self.assertEqual(rc, 0)
        self.assertIn("Return-to-human signal cleared", buf.getvalue())


# ---------------------------------------------------------------------------
# Subcommand: nothing-to-plan
# ---------------------------------------------------------------------------

class NothingToPlanTests(unittest.TestCase):
    def test_prints_deprecation(self):
        with _capture_stdout() as buf:
            rc = coordination.cmd_nothing_to_plan(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertIn("deprecated", buf.getvalue())


# ---------------------------------------------------------------------------
# Subcommand: lock-planner family
# ---------------------------------------------------------------------------

class LockPlannerTests(unittest.TestCase):
    def setUp(self):
        self.ctx = _make_ctx()
        self.ctx._planner_lock_issue = 100
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        # Redirect lock comment file.
        self._patches = []

        def fake_clf(self_arg):
            return Path(self._tmp.name) / "lock-comment.id"

        # Override the property on the dataclass instance.
        self.ctx.__dict__["_planner_lock_comment_override"] = (
            Path(self._tmp.name) / "lock-comment.id")
        # Patch the property on the class to read our override.
        p = mock.patch.object(
            coordination.CoordinationContext,
            "planner_lock_comment_file",
            new_callable=mock.PropertyMock,
            return_value=Path(self._tmp.name) / "lock-comment.id",
        )
        p.start()
        self.addCleanup(p.stop)

    def test_lock_acquire_when_solo(self):
        # POST returns id=500; subsequent paginate returns only that id.
        post_resp = fake_response(201, body={"id": 500})
        page = fake_response(200, body=[{
            "id": 500, "body": "planner-lock-attempt: sess-A",
            "created_at": "2026-05-10T14:30:00Z",
        }])
        with _capture_stdout() as buf, \
             mock.patch("pod.coordination._iso_to_epoch",
                        return_value=int(time.time())), \
             patch_client(routes={
                 ("POST", "/repos/owner/repo/issues/100/comments"): post_resp,
                 ("GET", "/repos/owner/repo/issues/100/comments"): page,
             }), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_lock_planner(self.ctx, [])
        self.assertEqual(rc, 0)
        self.assertIn("Planner lock acquired", buf.getvalue())

    def test_lock_lost_when_someone_earlier(self):
        post_resp = fake_response(201, body={"id": 600})
        page = fake_response(200, body=[
            {"id": 500, "body": "planner-lock-attempt: other",
             "created_at": "2026-05-10T14:29:00Z"},
            {"id": 600, "body": "planner-lock-attempt: sess-A",
             "created_at": "2026-05-10T14:30:00Z"},
        ])
        deletes: list[str] = []

        # Track delete calls.
        from _gh_helpers import _FakeClient
        client = _FakeClient(routes={
            ("POST", "/repos/owner/repo/issues/100/comments"): post_resp,
            ("GET", "/repos/owner/repo/issues/100/comments"): page,
            ("DELETE", "/repos/owner/repo/issues/comments/600"):
                fake_response(204),
        })
        with _capture_stdout() as buf, \
             mock.patch("pod.coordination._iso_to_epoch",
                        return_value=int(time.time())), \
             patch_client(client), \
             mock.patch("time.sleep"):
            rc = coordination.cmd_lock_planner(self.ctx, [])
        self.assertEqual(rc, 1)
        self.assertIn("Another planner is active", buf.getvalue())
        # The losing session should have deleted its own comment.
        delete_paths = [c["path"] for c in client.calls
                        if c["method"] == "DELETE"]
        self.assertIn("/repos/owner/repo/issues/comments/600",
                      delete_paths)

    def test_lock_status_locked(self):
        page = fake_response(200, body=[{
            "id": 500, "body": "planner-lock-attempt: x",
            "created_at": "2026-05-10T14:30:00Z",
        }])
        with _capture_stdout() as buf, \
             mock.patch("pod.coordination._iso_to_epoch",
                        return_value=int(time.time())), \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/100/comments"): page,
             }):
            rc = coordination.cmd_lock_status(self.ctx, [])
        self.assertEqual(rc, 0)
        self.assertEqual(buf.getvalue().strip(), "locked")

    def test_lock_status_unlocked(self):
        page = fake_response(200, body=[])
        with _capture_stdout() as buf, \
             patch_client(routes={
                 ("GET", "/repos/owner/repo/issues/100/comments"): page,
             }):
            rc = coordination.cmd_lock_status(self.ctx, [])
        self.assertEqual(rc, 1)
        self.assertEqual(buf.getvalue().strip(), "unlocked")


# ---------------------------------------------------------------------------
# Subcommand: add-dep
# ---------------------------------------------------------------------------

class AddDepTests(unittest.TestCase):
    def test_already_has_dep_no_edit(self):
        edits: list[tuple] = []

        def handler(*argv):
            if argv[:2] == ("issue", "view") and "body" in argv:
                return _gh_result(
                    stdout="Some body.\ndepends-on: #5\nmore text",
                )
            if argv[:2] == ("issue", "view") and "state" in argv:
                return _gh_result(stdout="OPEN")
            if argv[:2] == ("issue", "edit"):
                edits.append(argv)
                return _gh_result()
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_add_dep(_make_ctx(), ["10", "5"])
        self.assertEqual(rc, 0)
        self.assertIn("already has depends-on: #5", buf.getvalue())
        # No body edit, but blocked label may be added (dep is OPEN).
        body_edits = [e for e in edits if "--body" in e]
        self.assertEqual(body_edits, [])

    def test_new_dep_appends_to_body_and_blocks(self):
        captured_body = None

        def handler(*argv):
            nonlocal captured_body
            if argv[:2] == ("issue", "view") and "body" in argv:
                return _gh_result(stdout="initial body")
            if argv[:2] == ("issue", "view") and "state" in argv:
                return _gh_result(stdout="OPEN")
            if argv[:2] == ("issue", "edit") and "--body" in argv:
                idx = list(argv).index("--body")
                captured_body = argv[idx + 1]
                return _gh_result()
            if argv[:2] == ("issue", "edit"):
                return _gh_result()
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_add_dep(_make_ctx(), ["10", "7"])
        self.assertEqual(rc, 0)
        self.assertIsNotNone(captured_body)
        self.assertIn("depends-on: #7", captured_body)
        self.assertIn("Added depends-on: #7 to body of issue #10",
                       buf.getvalue())
        self.assertIn("Added blocked label", buf.getvalue())

    def test_closed_dep_no_blocked_label(self):
        def handler(*argv):
            if argv[:2] == ("issue", "view") and "body" in argv:
                return _gh_result(stdout="initial")
            if argv[:2] == ("issue", "view") and "state" in argv:
                return _gh_result(stdout="CLOSED")
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_add_dep(_make_ctx(), ["10", "7"])
        self.assertEqual(rc, 0)
        self.assertIn("already closed; blocked label not added",
                       buf.getvalue())


# ---------------------------------------------------------------------------
# Subcommand: read-issue (provenance gate)
# ---------------------------------------------------------------------------

class ReadIssueTests(unittest.TestCase):
    def test_provenance_fail_returns_1(self):
        with mock.patch.object(coordination, "_check_provenance",
                                return_value=(False, "untrusted")):
            with _capture_stdout(), patch_client():
                rc = coordination.cmd_read_issue(_make_ctx(), ["42"])
        self.assertEqual(rc, 1)

    def test_provenance_ok_emits_body(self):
        with mock.patch.object(coordination, "_check_provenance",
                                return_value=(True, "")):
            def handler(*argv):
                return _gh_result(stdout="hello body\n")
            with _capture_stdout() as buf, patch_client(
                    gh_cli_handler=handler):
                rc = coordination.cmd_read_issue(_make_ctx(), ["42"])
        self.assertEqual(rc, 0)
        self.assertIn("hello body", buf.getvalue())


# ---------------------------------------------------------------------------
# Subcommand: check-blocked
# ---------------------------------------------------------------------------

class CheckBlockedTests(unittest.TestCase):
    def test_orphan_blocked_removed(self):
        edits = []
        comments = []

        def handler(*argv):
            if argv[:2] == ("issue", "list"):
                return _gh_result(stdout=json.dumps(
                    [{"number": 10, "body": "no depends-on lines"}]))
            if argv[:2] == ("issue", "edit"):
                edits.append(argv)
                return _gh_result()
            if argv[:2] == ("issue", "comment"):
                comments.append(argv)
                return _gh_result()
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_check_blocked(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertIn("Cleared orphan blocked on #10", buf.getvalue())
        # We removed the label and posted an explanation comment.
        self.assertEqual(len(edits), 1)
        self.assertIn("--remove-label", edits[0])
        self.assertEqual(len(comments), 1)

    def test_unblocks_when_all_deps_closed(self):
        edits = []

        def handler(*argv):
            if argv[:2] == ("issue", "list"):
                return _gh_result(stdout=json.dumps([{
                    "number": 10,
                    "body": "depends-on: #5\ndepends-on: #6",
                }]))
            if argv[:2] == ("issue", "view") and "state" in argv:
                return _gh_result(stdout="CLOSED")
            if argv[:2] == ("issue", "edit"):
                edits.append(argv)
                return _gh_result()
            return _gh_result()

        with _capture_stdout() as buf, patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_check_blocked(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertIn("Unblocked issue #10", buf.getvalue())
        self.assertIn("--remove-label", edits[0])

    def test_keeps_blocked_when_dep_still_open(self):
        edits = []

        def handler(*argv):
            if argv[:2] == ("issue", "list"):
                return _gh_result(stdout=json.dumps([{
                    "number": 10,
                    "body": "depends-on: #5",
                }]))
            if argv[:2] == ("issue", "view") and "state" in argv:
                return _gh_result(stdout="OPEN")
            if argv[:2] == ("issue", "edit"):
                edits.append(argv)
                return _gh_result()
            return _gh_result()

        with _capture_stdout(), patch_client(gh_cli_handler=handler):
            rc = coordination.cmd_check_blocked(_make_ctx(), [])
        self.assertEqual(rc, 0)
        self.assertEqual(edits, [])


# ---------------------------------------------------------------------------
# Subcommand: create-pr (protected-files guard)
# ---------------------------------------------------------------------------

class CreatePrProtectedFilesTests(unittest.TestCase):
    def test_blocks_pr_touching_protected_file(self):
        def git_run(args, **kw):
            if args[:2] == ["git", "merge-base"]:
                return _gh_result(stdout="abc123")
            if args[:2] == ["git", "diff"]:
                return _gh_result(stdout="PLAN.md\n")
            if args[:2] == ["git", "push"]:
                return _gh_result()
            return _gh_result()

        with patch_client(), \
             mock.patch.object(coordination.subprocess, "run",
                                side_effect=git_run):
            with self.assertRaises(SystemExit) as cm:
                coordination.cmd_create_pr(_make_ctx(), ["42"])
        self.assertEqual(cm.exception.code, 1)

    def test_master_branch_refused(self):
        ctx = _make_ctx(branch="main")
        with patch_client():
            with self.assertRaises(SystemExit):
                coordination.cmd_create_pr(ctx, ["42"])


# ---------------------------------------------------------------------------
# Dispatch / main
# ---------------------------------------------------------------------------

class ReleaseOrphanClaimsTests(unittest.TestCase):
    """Liveness-based orphan-claim sweep — releases claims whose owning
    session UUID is no longer in `.pod/agents/`, regardless of age."""

    def _ctx_with_agents(self, tmp: Path, live_uuids: list[str]) -> coordination.CoordinationContext:
        (tmp / ".pod" / "agents").mkdir(parents=True)
        for i, u in enumerate(live_uuids):
            (tmp / ".pod" / "agents" / f"agent{i}.json").write_text(json.dumps({
                "short_id": f"abcd{i}",
                "uuid": u,
                "status": "running",
                "pid": os.getpid(),
            }))
        ctx = _make_ctx()
        ctx.git_toplevel = str(tmp)
        return ctx

    def test_releases_when_owner_not_in_local_agents(self):
        with tempfile.TemporaryDirectory() as td:
            ctx = self._ctx_with_agents(Path(td), live_uuids=["live-uuid"])
            calls = []
            def fake_run(*argv, **kw):
                calls.append(argv)
                if argv[:2] == ("issue", "list"):
                    return _gh_result(stdout=json.dumps([
                        {"number": 42, "title": "orphan"},
                    ]))
                if argv[:2] == ("issue", "view"):
                    return _gh_result(stdout=json.dumps(
                        "Claimed by session `dead-uuid` on branch `agent/x`"))
                return _gh_result()
            with patch_client(gh_cli_handler=fake_run), _capture_stdout() as out:
                rc = coordination.cmd_release_orphan_claims(ctx, [])
            self.assertEqual(rc, 0)
            # Saw label-remove + comment for the orphan.
            verbs = [(c[0], c[1], c[2]) for c in calls
                     if len(c) >= 3 and c[0] == "issue"]
            self.assertIn(("issue", "edit", "42"), verbs)
            self.assertIn(("issue", "comment", "42"), verbs)
            self.assertIn("orphan", out.getvalue().lower())

    def test_skips_when_owner_still_in_local_agents(self):
        with tempfile.TemporaryDirectory() as td:
            ctx = self._ctx_with_agents(Path(td), live_uuids=["live-uuid"])
            calls = []
            def fake_run(*argv, **kw):
                calls.append(argv)
                if argv[:2] == ("issue", "list"):
                    return _gh_result(stdout=json.dumps([
                        {"number": 42, "title": "still-claimed"},
                    ]))
                if argv[:2] == ("issue", "view"):
                    return _gh_result(stdout=json.dumps(
                        "Claimed by session `live-uuid` on branch `agent/x`"))
                return _gh_result()
            with patch_client(gh_cli_handler=fake_run), _capture_stdout():
                rc = coordination.cmd_release_orphan_claims(ctx, [])
            self.assertEqual(rc, 0)
            verbs = [(c[0], c[1], c[2]) for c in calls
                     if len(c) >= 3 and c[0] == "issue"]
            self.assertNotIn(("issue", "edit", "42"), verbs)
            self.assertNotIn(("issue", "comment", "42"), verbs)

    def test_skips_dead_status_agents(self):
        # Agents marked dead/stopped/killed are not considered live owners.
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / ".pod" / "agents").mkdir(parents=True)
            (Path(td) / ".pod" / "agents" / "a.json").write_text(json.dumps({
                "short_id": "abcd0", "uuid": "ghost-uuid",
                "status": "dead", "pid": 0,
            }))
            ctx = _make_ctx()
            ctx.git_toplevel = td
            calls = []
            def fake_run(*argv, **kw):
                calls.append(argv)
                if argv[:2] == ("issue", "list"):
                    return _gh_result(stdout=json.dumps([
                        {"number": 7, "title": "orphan"},
                    ]))
                if argv[:2] == ("issue", "view"):
                    return _gh_result(stdout=json.dumps(
                        "Claimed by session `ghost-uuid` on branch `agent/x`"))
                return _gh_result()
            with patch_client(gh_cli_handler=fake_run), _capture_stdout():
                rc = coordination.cmd_release_orphan_claims(ctx, [])
            self.assertEqual(rc, 0)
            verbs = [(c[0], c[1], c[2]) for c in calls
                     if len(c) >= 3 and c[0] == "issue"]
            self.assertIn(("issue", "edit", "7"), verbs)

    def test_refuses_without_agents_directory(self):
        # No `.pod/agents/` → can't make a liveness decision → exit 1.
        with tempfile.TemporaryDirectory() as td:
            ctx = _make_ctx()
            ctx.git_toplevel = td
            with patch_client():
                with contextlib.redirect_stderr(io.StringIO()):
                    rc = coordination.cmd_release_orphan_claims(ctx, [])
            self.assertEqual(rc, 1)


class DispatchTests(unittest.TestCase):
    def test_unknown_command_dies(self):
        with mock.patch.object(coordination, "_ensure_auth"):
            with self.assertRaises(SystemExit):
                coordination.main(["definitely-not-a-command"])

    def test_empty_argv_dies(self):
        with self.assertRaises(SystemExit):
            coordination.main([])


if __name__ == "__main__":
    unittest.main()
