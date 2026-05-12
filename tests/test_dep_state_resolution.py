"""Tests for `fetch_issue_states` — the aliased-batch GraphQL helper
that `fetch_blocked_deps` uses to resolve `depends-on: #N` references.

This replaces the older `openIssueNumbers` enumeration which had a
silent 100-oldest cap and dropped annotations on repos with more than
100 open issues. The new code resolves dep states via a per-number
GraphQL lookup, batched (25 aliases per POST) and TTL-cached on disk.
"""
from __future__ import annotations

import json
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli

from _gh_helpers import fake_response, patch_client


class BuildIssueStateBatchQueryTests(unittest.TestCase):
    def test_generates_n_aliases_and_variable_decls(self):
        q = cli._build_issue_state_batch_query(3)
        # 3 aliases.
        for k in (0, 1, 2):
            self.assertIn(f"i{k}: issue(number: $num{k})", q)
        # Variable decls in the query signature.
        self.assertIn("$num0: Int!", q)
        self.assertIn("$num1: Int!", q)
        self.assertIn("$num2: Int!", q)
        # No fourth alias.
        self.assertNotIn("i3:", q)

    def test_only_requests_state_and_number(self):
        q = cli._build_issue_state_batch_query(1)
        # Should NOT request body / labels / comments — those are heavy and
        # unnecessary. State + number is enough for dep-resolution.
        self.assertNotIn("body", q)
        self.assertNotIn("labels", q)
        self.assertNotIn("comments", q)
        self.assertRegex(q, r"i0: issue\(number: \$num0\) \{\s*number\s+state\s*\}")


class FetchIssueStatesTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.cache_path = Path(self._tmp.name) / "issue-state-cache.json"
        p = mock.patch.object(cli, "_issue_state_cache_path",
                               return_value=self.cache_path)
        p.start()
        self.addCleanup(p.stop)

    def _make_response(self, states_by_num):
        """Build a GraphQL response body in which `i{k}` aliases map to
        the supplied states_by_num. Order of nums in the response matches
        insertion order of states_by_num."""
        repo = {}
        for k, (n, s) in enumerate(states_by_num.items()):
            repo[f"i{k}"] = {"number": n, "state": s} if s is not None else None
        return {"data": {"repository": repo}}

    def test_empty_input_returns_empty_dict(self):
        out = cli.fetch_issue_states("o/r", [])
        self.assertEqual(out, {})

    def test_single_batch_resolves_all_states(self):
        body = self._make_response({100: "OPEN", 101: "CLOSED", 102: "OPEN"})
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body=body),
        }):
            out = cli.fetch_issue_states("o/r", [100, 101, 102])
        self.assertEqual(out, {100: "OPEN", 101: "CLOSED", 102: "OPEN"})

    def test_multi_batch_when_over_25(self):
        # 26 nums → two GraphQL POSTs. `patch_client` supports a list of
        # responses per route, dispensed in call order.
        nums = list(range(1000, 1026))
        responses = [
            fake_response(200, body=self._make_response(
                {n: "OPEN" for n in nums[:25]})),
            fake_response(200, body=self._make_response(
                {nums[25]: "CLOSED"})),
        ]
        with patch_client(routes={
            ("POST", "/graphql"): responses,
        }) as client:
            out = cli.fetch_issue_states("o/r", nums)
        calls = [c for c in client.calls if c.get("path") == "/graphql"]
        self.assertEqual(len(calls), 2)
        v1 = calls[0]["json"]["variables"]
        v2 = calls[1]["json"]["variables"]
        self.assertEqual({k for k in v1 if k.startswith("num")},
                         {f"num{k}" for k in range(25)})
        self.assertEqual({k for k in v2 if k.startswith("num")}, {"num0"})
        self.assertEqual(out[1000], "OPEN")
        self.assertEqual(out[1025], "CLOSED")

    def test_cache_hit_skips_graphql(self):
        # Seed cache directly.
        import time as _time
        cli._save_issue_state_cache({
            cli._issue_state_cache_key("o/r", 7): {
                "state": "OPEN", "fetched_at": _time.time(),
            },
            cli._issue_state_cache_key("o/r", 8): {
                "state": "CLOSED", "fetched_at": _time.time(),
            },
        })
        # No routes registered — any GraphQL call would return the
        # default 503 response. We assert that no POST happened at all.
        with patch_client() as client:
            out = cli.fetch_issue_states("o/r", [7, 8])
        self.assertEqual(out, {7: "OPEN", 8: "CLOSED"})
        gql_calls = [c for c in client.calls
                     if c.get("path") == "/graphql"]
        self.assertEqual(gql_calls, [])

    def test_expired_cache_triggers_refetch(self):
        # Seed with an expired entry.
        cli._save_issue_state_cache({
            cli._issue_state_cache_key("o/r", 7): {
                "state": "OPEN",
                "fetched_at": 0.0,  # epoch — definitely expired
            },
        })
        body = self._make_response({7: "CLOSED"})
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body=body),
        }):
            out = cli.fetch_issue_states("o/r", [7])
        self.assertEqual(out, {7: "CLOSED"})

    def test_transport_failure_omits_chunk(self):
        # GraphQL returns 500 → caller must NOT receive any state for
        # those nums. (Fail-closed: `fetch_blocked_deps` then drops them.)
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(500, body={}),
        }):
            out = cli.fetch_issue_states("o/r", [9, 10])
        self.assertEqual(out, {})

    def test_writes_cache_for_successful_lookups(self):
        body = self._make_response({100: "OPEN"})
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body=body),
        }):
            cli.fetch_issue_states("o/r", [100])
        raw = json.loads(self.cache_path.read_text())
        key = cli._issue_state_cache_key("o/r", 100)
        self.assertIn(key, raw)
        self.assertEqual(raw[key]["state"], "OPEN")
        self.assertGreater(raw[key]["fetched_at"], 0.0)


class TuiRefreshQueryShapeTests(unittest.TestCase):
    """Defence-in-depth: the GraphQL query string must include the
    orderBy clauses we added, otherwise a future refactor could quietly
    revert to oldest-first and the TUI would silently drop newest items
    again. Also: `openIssueNumbers` must NOT be present (it was removed
    in favour of `fetch_issue_states`)."""

    def test_open_agent_plan_orders_newest_first(self):
        q = cli._TUI_REFRESH_QUERY
        # Pattern matches the openAgentPlan sub-query and its orderBy.
        self.assertRegex(
            q,
            r"openAgentPlan: issues\([^)]*orderBy:\s*\{field:\s*CREATED_AT,"
            r"\s*direction:\s*DESC\}",
        )

    def test_open_human_oversight_orders_newest_first(self):
        self.assertRegex(
            cli._TUI_REFRESH_QUERY,
            r"openHumanOversight: issues\([^)]*orderBy:\s*\{field:\s*CREATED_AT,"
            r"\s*direction:\s*DESC\}",
        )

    def test_blocked_orders_by_updated_desc(self):
        self.assertRegex(
            cli._TUI_REFRESH_QUERY,
            r"blocked: issues\([^)]*orderBy:\s*\{field:\s*UPDATED_AT,"
            r"\s*direction:\s*DESC\}",
        )

    def test_has_pr_issues_orders_by_updated_desc(self):
        self.assertRegex(
            cli._TUI_REFRESH_QUERY,
            r"hasPrIssues: issues\([^)]*orderBy:\s*\{field:\s*UPDATED_AT,"
            r"\s*direction:\s*DESC\}",
        )

    def test_open_agent_plan_cap_is_at_github_max(self):
        # GitHub's GraphQL `first` is capped at 100 per connection;
        # going above silently errors the entire batched query (HTTP
        # 200 + `{"errors": [...]}` body, hard to debug from the
        # caller side). Stay at 100 here. Hex-scale repos rely on
        # `orderBy: CREATED_AT DESC` for newest-first visibility.
        self.assertRegex(
            cli._TUI_REFRESH_QUERY,
            r"openAgentPlan: issues\(first:\s*100,",
        )

    def test_open_issue_numbers_removed(self):
        self.assertNotIn("openIssueNumbers", cli._TUI_REFRESH_QUERY)

    def test_cache_schema_is_2(self):
        self.assertEqual(cli._TUI_CACHE_SCHEMA, 2)


if __name__ == "__main__":
    unittest.main()
