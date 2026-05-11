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

from _gh_helpers import patch_client


def _gh_result(stdout=""):
    r = mock.Mock()
    r.returncode = 0
    r.stdout = stdout
    r.stderr = ""
    return r


class FetchBlockedDepsQueryTests(unittest.TestCase):
    def test_query_does_not_filter_to_agent_plan(self):
        captured = {}

        def gh_cli_handler(*argv):
            # First call: list blocked. We capture it then return [].
            if "list" in argv and "blocked" in argv:
                captured["argv"] = ("gh",) + argv
                return _gh_result("[]")
            return _gh_result("[]")

        with patch_client(gh_cli_handler=gh_cli_handler):
            cli.fetch_blocked_deps()

        argv = list(captured["argv"])
        self.assertEqual(argv[0], "gh")
        self.assertIn("--label", argv)
        label_args = [argv[i + 1] for i, a in enumerate(argv) if a == "--label"]
        self.assertEqual(label_args, ["blocked"])

    def test_human_oversight_blocked_issue_yields_deps(self):
        body = ("HO-2 work item.\n\n"
                "depends-on: #2563\n"
                "depends-on: #2564\n")
        list_payload = json.dumps([{"number": 2565, "body": body}])
        open_payload = json.dumps([2563, 2564])

        calls = iter([list_payload, open_payload])

        def gh_cli_handler(*argv):
            return _gh_result(next(calls))

        with patch_client(gh_cli_handler=gh_cli_handler):
            result = cli.fetch_blocked_deps()

        self.assertEqual(result, {2565: [2563, 2564]})


class FetchHasPrLinksTests(unittest.TestCase):
    """Three-step lookup: list has-pr issues → look up each issue's
    closedByPullRequestsReferences → look up each referenced PR's state
    via `gh pr view`. The reference objects do NOT contain a `state`
    field, so a one-shot `select(.state == "OPEN")` filter would
    always return empty and the housekeeping cycle would clear
    `has-pr` from every legitimate issue."""

    def _stub_handler(self, calls):
        """Build a gh_cli handler that dispatches by argv shape, returning
        canned stdout from the `calls` dict keyed on (argv[0], argv[1])."""
        def handler(*argv):
            key = (argv[0], argv[1])
            if key == ("issue", "list"):
                return _gh_result(calls.pop("issue_list", "[]"))
            if key == ("issue", "view"):
                return _gh_result(calls.pop(f"issue_view_{argv[2]}", "[]"))
            if key == ("pr", "view"):
                return _gh_result(calls.pop(f"pr_view_{argv[2]}", "UNKNOWN"))
            return _gh_result("")
        return handler

    def test_records_open_linked_pr(self):
        # #3006 has an open linked PR #3015 — must show up.
        with patch_client(gh_cli_handler=self._stub_handler({
            "issue_list": json.dumps([{"number": 3006}]),
            "issue_view_3006": json.dumps([3015]),
            "pr_view_3015": "OPEN",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {3006: [3015]})

    def test_merged_linked_pr_yields_empty_list_orphan(self):
        # #3016 has a linked PR but it's already merged — orphan; the
        # entry must still be present (empty list) so the TUI can
        # render `[PR ?]` and the housekeeping cycle can clean up.
        with patch_client(gh_cli_handler=self._stub_handler({
            "issue_list": json.dumps([{"number": 3016}]),
            "issue_view_3016": json.dumps([3020]),
            "pr_view_3020": "MERGED",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {3016: []})

    def test_orphan_with_no_linked_prs_at_all(self):
        # The hand-applied `has-pr` case from the original incident:
        # closedByPullRequestsReferences itself is empty.
        with patch_client(gh_cli_handler=self._stub_handler({
            "issue_list": json.dumps([{"number": 2564}]),
            "issue_view_2564": "[]",
        })):
            self.assertEqual(cli.fetch_has_pr_links(), {2564: []})

    def test_open_and_closed_links_keeps_only_open(self):
        # The relevant filter: an issue may have one merged PR
        # (historically) and one open PR (the in-flight one). Only the
        # open one should appear in the result.
        with patch_client(gh_cli_handler=self._stub_handler({
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
