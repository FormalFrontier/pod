"""Tests for the TUI's orphan-label helpers (B2 burner rewrite).

After the B2 rewrite, `fetch_blocked_deps`, `fetch_has_pr_links`, and
`fetch_issues_and_prs` all read from a single batched GraphQL query
(`_tui_refresh_batch`). These tests stub that helper directly so
each fetch_* is exercised in isolation, mirroring the in-memory shape
the layer would produce.
"""
import json
import unittest
from unittest import mock

from pod import cli


def _label_nodes(*names):
    return {"nodes": [{"name": n} for n in names]}


def _issue(num, *, title="", labels=(), body="",
           created_at="2026-05-09T00:00:00Z",
           updated_at="2026-05-10T00:00:00Z",
           closed_at=None):
    iss = {
        "number": num,
        "title": title,
        "labels": _label_nodes(*labels),
        "createdAt": created_at,
        "updatedAt": updated_at,
    }
    if body is not None:
        iss["body"] = body
    if closed_at is not None:
        iss["closedAt"] = closed_at
    return iss


def _pr_node(num, *, title="", labels=(), state="OPEN",
             checks_state=None,
             created_at="2026-05-09T00:00:00Z",
             updated_at="2026-05-10T00:00:00Z"):
    return {
        "number": num,
        "title": title,
        "state": state,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "closedAt": None,
        "mergedAt": None,
        "labels": _label_nodes(*labels),
        "commits": {"nodes": [{
            "commit": {
                "statusCheckRollup": (
                    {"state": checks_state} if checks_state else None
                ),
            }
        }]},
    }


def _has_pr_node(issue_num, *, refs):
    """Build a hasPrIssues node; refs is a list of (pr_num, state)."""
    return {
        "number": issue_num,
        "closedByPullRequestsReferences": {
            "nodes": [{"number": n, "state": s} for n, s in refs],
        },
    }


def _patch_batch(repo_node):
    """Patch the batched fetch to return our stubbed repository node."""
    return mock.patch.object(cli, "_tui_refresh_batch",
                              return_value=repo_node)


def _patch_states(states_by_num):
    """Patch `fetch_issue_states` to return the supplied {num: state} dict."""
    return mock.patch.object(
        cli, "fetch_issue_states",
        side_effect=lambda repo, nums: {n: states_by_num.get(n, "")
                                         for n in nums})


def _patch_repo(slug="o/r"):
    return mock.patch.object(cli, "_get_repo", return_value=slug)


class FetchBlockedDepsTests(unittest.TestCase):
    def test_no_blocked_issues_yields_empty(self):
        with _patch_batch({"blocked": {"nodes": []}}), _patch_repo():
            self.assertEqual(cli.fetch_blocked_deps(), {})

    def test_includes_blocked_from_any_family_label(self):
        # The blocked field doesn't filter by agent-plan vs directive,
        # so HO-2 (#2565 in the kim-em/hex repo) shows up here too.
        body = ("HO-2 work item.\n\n"
                "depends-on: #2563\n"
                "depends-on: #2564\n")
        with _patch_batch({"blocked": {"nodes": [_issue(2565, body=body)]}}), \
             _patch_states({2563: "OPEN", 2564: "OPEN"}), \
             _patch_repo():
            result = cli.fetch_blocked_deps()
        self.assertEqual(result, {2565: [2563, 2564]})

    def test_drops_closed_deps(self):
        body = "depends-on: #100\ndepends-on: #101\n"
        with _patch_batch({"blocked": {"nodes": [_issue(50, body=body)]}}), \
             _patch_states({100: "OPEN", 101: "CLOSED"}), \
             _patch_repo():
            result = cli.fetch_blocked_deps()
        self.assertEqual(result, {50: [100]})

    def test_omits_issue_with_all_deps_closed(self):
        body = "depends-on: #100\n"
        with _patch_batch({"blocked": {"nodes": [_issue(50, body=body)]}}), \
             _patch_states({100: "CLOSED"}), \
             _patch_repo():
            result = cli.fetch_blocked_deps()
        self.assertEqual(result, {})

    def test_lookup_failure_fail_closed(self):
        # A dep whose state lookup returned "" (transport failure) must
        # NOT be reported as open — that would let a closed-issue
        # annotation leak through.
        body = "depends-on: #200\ndepends-on: #201\n"
        with _patch_batch({"blocked": {"nodes": [_issue(60, body=body)]}}), \
             _patch_states({200: "OPEN", 201: ""}), \
             _patch_repo():
            result = cli.fetch_blocked_deps()
        self.assertEqual(result, {60: [200]})

    def test_resolves_deps_outside_any_cap(self):
        # Regression: pre-fix, deps were resolved against `openIssueNumbers`
        # which capped at 100 oldest open issues. Recent deps were silently
        # dropped, hiding `Blocked on #N` annotations. The new code passes
        # whatever dep numbers it parses out of bodies directly to
        # `fetch_issue_states`, so there is no cap.
        body = "depends-on: #99999\n"
        recv: list[int] = []

        def capture(repo, nums):
            recv.extend(nums)
            return {n: "OPEN" for n in nums}

        with _patch_batch({"blocked": {"nodes": [_issue(7, body=body)]}}), \
             mock.patch.object(cli, "fetch_issue_states",
                               side_effect=capture), \
             _patch_repo():
            result = cli.fetch_blocked_deps()
        self.assertEqual(result, {7: [99999]})
        self.assertEqual(recv, [99999])


class FetchHasPrLinksTests(unittest.TestCase):
    def test_records_open_linked_pr(self):
        with _patch_batch({
            "hasPrIssues": {"nodes": [
                _has_pr_node(3006, refs=[(3015, "OPEN")]),
            ]},
        }):
            self.assertEqual(cli.fetch_has_pr_links(), {3006: [3015]})

    def test_merged_linked_pr_yields_empty_list_orphan(self):
        with _patch_batch({
            "hasPrIssues": {"nodes": [
                _has_pr_node(3016, refs=[(3020, "MERGED")]),
            ]},
        }):
            self.assertEqual(cli.fetch_has_pr_links(), {3016: []})

    def test_orphan_with_no_linked_prs_at_all(self):
        with _patch_batch({
            "hasPrIssues": {"nodes": [
                _has_pr_node(2564, refs=[]),
            ]},
        }):
            self.assertEqual(cli.fetch_has_pr_links(), {2564: []})

    def test_open_and_closed_links_keeps_only_open(self):
        with _patch_batch({
            "hasPrIssues": {"nodes": [
                _has_pr_node(4000, refs=[(4001, "MERGED"),
                                          (4002, "OPEN")]),
            ]},
        }):
            self.assertEqual(cli.fetch_has_pr_links(), {4000: [4002]})

    def test_no_has_pr_issues_yields_empty(self):
        with _patch_batch({"hasPrIssues": {"nodes": []}}):
            self.assertEqual(cli.fetch_has_pr_links(), {})


class FetchIssuesAndPrsTests(unittest.TestCase):
    def test_returns_open_issues_and_prs(self):
        with _patch_batch({
            "openAgentPlan": {"nodes": [
                _issue(10, title="open agent-plan",
                        labels=["agent-plan"]),
            ]},
            "openDirective": {"nodes": [
                _issue(20, title="open directive",
                        labels=["directive"]),
            ]},
            "closedAgentPlan": {"nodes": []},
            "closedDirective": {"nodes": []},
            "pullRequests": {"nodes": [
                _pr_node(30, title="pass pr", state="OPEN",
                          checks_state="SUCCESS"),
                _pr_node(31, title="fail pr", state="OPEN",
                          checks_state="FAILURE"),
            ]},
        }):
            items = cli.fetch_issues_and_prs()
        # Sorted by -number, with issue before pr at same number (none here).
        nums = [(it.kind, it.number) for it in items]
        self.assertIn(("issue", 10), nums)
        self.assertIn(("issue", 20), nums)
        self.assertIn(("pr", 30), nums)
        self.assertIn(("pr", 31), nums)
        # Highest number first.
        self.assertEqual(items[0].number, 31)
        # CI status decoded.
        pr_pass = next(i for i in items if i.kind == "pr" and i.number == 30)
        pr_fail = next(i for i in items if i.kind == "pr" and i.number == 31)
        self.assertEqual(pr_pass.ci_status, "pass")
        self.assertEqual(pr_fail.ci_status, "fail")


class BatchCacheTests(unittest.TestCase):
    def test_batch_cached_within_ttl(self):
        # Force a clean cache.
        cli._TUI_REFRESH_CACHE = (0.0, None)
        from _gh_helpers import fake_response, patch_client
        graphql_body = {"data": {"repository": {
            "openAgentPlan": {"nodes": []},
            "openDirective": {"nodes": []},
            "closedAgentPlan": {"nodes": []},
            "closedDirective": {"nodes": []},
            "blocked": {"nodes": []},
            "hasPrIssues": {"nodes": []},
            "pullRequests": {"nodes": []},
        }}}
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body=graphql_body),
        }) as client, mock.patch.object(cli, "_get_repo",
                                         return_value="o/r"):
            cli.fetch_issues_and_prs()
            cli.fetch_blocked_deps()
            cli.fetch_has_pr_links()
        # All three fetch_* called _tui_refresh_batch — but only one
        # GraphQL request should have hit the wire (cached for the rest).
        graphql_calls = [c for c in client.calls
                         if c["method"] == "POST" and c["path"] == "/graphql"]
        self.assertEqual(len(graphql_calls), 1)


if __name__ == "__main__":
    unittest.main()
