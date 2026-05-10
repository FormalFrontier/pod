"""Tests for the TUI's orphan-label helpers.

`fetch_blocked_deps` must query *all* `blocked` issues (no
`--label agent-plan` filter), so `human-oversight + blocked` issues
get their `depends-on:` lines parsed and rendered.

`fetch_has_pr_links` returns, for every open `has-pr` issue, the list
of its currently-OPEN closing PRs. An empty list is preserved (it
signals an orphan label that the housekeeping loop will clean up).
"""
import json
import subprocess
import unittest
from unittest import mock

from pod import cli


class FetchBlockedDepsQueryTests(unittest.TestCase):
    def test_query_does_not_filter_to_agent_plan(self):
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = list(argv)
            r = mock.Mock()
            r.returncode = 0
            r.stdout = "[]"
            r.stderr = ""
            return r

        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            cli.fetch_blocked_deps()

        argv = captured["argv"]
        self.assertEqual(argv[0], "gh")
        self.assertIn("--label", argv)
        # The post-fix query must include `blocked` exactly once and must
        # NOT include `agent-plan` (which would exclude human-oversight
        # blocked issues like #2565 in the kim-em/hex repo).
        label_args = [argv[i + 1] for i, a in enumerate(argv) if a == "--label"]
        self.assertEqual(label_args, ["blocked"])

    def test_human_oversight_blocked_issue_yields_deps(self):
        body = ("HO-2 work item.\n\n"
                "depends-on: #2563\n"
                "depends-on: #2564\n")
        list_payload = json.dumps([{"number": 2565, "body": body}])
        # second call returns the open-issue numbers list
        open_payload = json.dumps([2563, 2564])

        calls = iter([list_payload, open_payload])

        def fake_run(argv, **kwargs):
            r = mock.Mock()
            r.returncode = 0
            r.stdout = next(calls)
            r.stderr = ""
            return r

        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            result = cli.fetch_blocked_deps()

        self.assertEqual(result, {2565: [2563, 2564]})


class FetchHasPrLinksTests(unittest.TestCase):
    """Three-step lookup: list has-pr issues → look up each issue's
    closedByPullRequestsReferences → look up each referenced PR's state
    via `gh pr view`. The reference objects do NOT contain a `state`
    field, so a one-shot `select(.state == "OPEN")` filter would
    always return empty and the housekeeping cycle would clear
    `has-pr` from every legitimate issue."""

    def _stub_run(self, calls):
        """Build a fake_run that dispatches by argv shape, returning
        canned stdout from the `calls` dict keyed on (argv[1], argv[2])."""
        def fake_run(argv, **kwargs):
            r = mock.Mock()
            r.returncode = 0
            r.stderr = ""
            key = (argv[1], argv[2])
            if key == ("issue", "list"):
                r.stdout = calls.pop("issue_list", "[]")
            elif key == ("issue", "view"):
                # argv[3] is the issue number string
                r.stdout = calls.pop(f"issue_view_{argv[3]}", "[]")
            elif key == ("pr", "view"):
                r.stdout = calls.pop(f"pr_view_{argv[3]}", "UNKNOWN")
            else:
                r.stdout = ""
            return r
        return fake_run

    def test_records_open_linked_pr(self):
        # #3006 has an open linked PR #3015 — must show up.
        with mock.patch.object(subprocess, "run", side_effect=self._stub_run({
            "issue_list": json.dumps([{"number": 3006}]),
            "issue_view_3006": json.dumps([3015]),
            "pr_view_3015": "OPEN",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {3006: [3015]})

    def test_merged_linked_pr_yields_empty_list_orphan(self):
        # #3016 has a linked PR but it's already merged — orphan; the
        # entry must still be present (empty list) so the TUI can
        # render `[PR ?]` and the housekeeping cycle can clean up.
        with mock.patch.object(subprocess, "run", side_effect=self._stub_run({
            "issue_list": json.dumps([{"number": 3016}]),
            "issue_view_3016": json.dumps([3020]),
            "pr_view_3020": "MERGED",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {3016: []})

    def test_orphan_with_no_linked_prs_at_all(self):
        # The hand-applied `has-pr` case from the original incident:
        # closedByPullRequestsReferences itself is empty.
        with mock.patch.object(subprocess, "run", side_effect=self._stub_run({
            "issue_list": json.dumps([{"number": 2564}]),
            "issue_view_2564": "[]",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {2564: []})

    def test_open_and_closed_links_keeps_only_open(self):
        # The relevant filter: an issue may have one merged PR
        # (historically) and one open PR (the in-flight one). Only the
        # open one should appear in the result.
        with mock.patch.object(subprocess, "run", side_effect=self._stub_run({
            "issue_list": json.dumps([{"number": 4000}]),
            "issue_view_4000": json.dumps([4001, 4002]),
            "pr_view_4001": "MERGED",
            "pr_view_4002": "OPEN",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {4000: [4002]})

    def test_no_has_pr_issues_returns_empty_without_view_calls(self):
        list_payload = "[]"
        view_called = False

        def fake_run(argv, **kwargs):
            nonlocal view_called
            if argv[:3] == ["gh", "issue", "view"] or argv[:3] == ["gh", "pr", "view"]:
                view_called = True
            r = mock.Mock()
            r.returncode = 0
            r.stdout = list_payload
            r.stderr = ""
            return r

        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            result = cli.fetch_has_pr_links()

        self.assertEqual(result, {})
        self.assertFalse(view_called)


if __name__ == "__main__":
    unittest.main()
