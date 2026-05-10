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
    def test_records_open_linked_pr(self):
        list_payload = json.dumps([{"number": 3006}])
        view_payload = json.dumps([3015])

        calls = iter([list_payload, view_payload])

        def fake_run(argv, **kwargs):
            r = mock.Mock()
            r.returncode = 0
            r.stdout = next(calls)
            r.stderr = ""
            return r

        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            result = cli.fetch_has_pr_links()

        self.assertEqual(result, {3006: [3015]})

    def test_orphan_has_pr_yields_empty_list(self):
        # The TUI relies on the *presence* of the key (with empty list)
        # to render `[PR ?]`, so the orphan stands out before housekeeping
        # clears it. The key must be retained, not dropped.
        list_payload = json.dumps([{"number": 2564}])
        view_payload = "[]"

        calls = iter([list_payload, view_payload])

        def fake_run(argv, **kwargs):
            r = mock.Mock()
            r.returncode = 0
            r.stdout = next(calls)
            r.stderr = ""
            return r

        with mock.patch.object(subprocess, "run", side_effect=fake_run):
            result = cli.fetch_has_pr_links()

        self.assertEqual(result, {2564: []})

    def test_no_has_pr_issues_returns_empty_without_view_calls(self):
        list_payload = "[]"
        view_called = False

        def fake_run(argv, **kwargs):
            nonlocal view_called
            if argv[:3] == ["gh", "issue", "view"]:
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
