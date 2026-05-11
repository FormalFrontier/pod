"""Tests for the burner-rewrite paths (B1-B4) — call-site reductions
that replace multi-call REST sequences with single GraphQL queries.

B1 — reconcile_untracked_github_claims  (cli.py)
B2 — fetch_issues_and_prs / fetch_blocked_deps / fetch_has_pr_links
     (cli.py, exercised by test_orphan_label_helpers.py:BatchCacheTests)
B3 — check_repo_security visibility path
     (cli.py, exercised by test_security.py)
B4 — fetch_issue_provenance  (cli.py)

This file focuses on B1 and B4, where the other burners' tests already
live alongside their original test files.
"""

from __future__ import annotations

import datetime
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import cli

from _gh_helpers import fake_response, patch_client


# ---------------------------------------------------------------------------
# B1 — claim-release sweep batched GraphQL
# ---------------------------------------------------------------------------

def _claim_node(num: int, *, labels=("agent-plan", "claimed"),
                 comments=()):
    """Build a single repository.issues.nodes[] entry."""
    return {
        "number": num,
        "state": "OPEN",
        "labels": {"nodes": [{"name": l} for l in labels]},
        "comments": {"nodes": list(comments)},
    }


def _claim_comment(session_uuid: str, short_id: str,
                    created_at: str) -> dict:
    return {
        "databaseId": int(time.time() * 1000),
        "createdAt": created_at,
        "body": f"Claimed by session `{session_uuid}` on branch "
                f"`agent/{short_id}`",
    }


def _iso(seconds_ago: int) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=seconds_ago)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class ReconcileBatchTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._patches = [
            mock.patch.object(cli, "_get_repo", return_value="o/r"),
            mock.patch.object(cli, "load_claim_history", return_value={}),
            mock.patch.object(cli, "_save_claim_history"),
            mock.patch.object(cli, "read_all_agents", return_value=[]),
            mock.patch.object(cli, "_claim_history_filelock",
                                new=mock.MagicMock()),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def _gql_response(self, *nodes):
        body = {"data": {"repository": {
            "issues": {"nodes": list(nodes)},
        }}}
        return fake_response(200, body=body)

    def test_skips_when_no_claimed_issues(self):
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(),
        }) as client:
            cli.reconcile_untracked_github_claims()
        # One GraphQL hit; no per-issue follow-ups.
        graphql_calls = [c for c in client.calls
                         if c["method"] == "POST" and c["path"] == "/graphql"]
        self.assertEqual(len(graphql_calls), 1)

    def test_releases_old_dead_claim(self):
        node = _claim_node(42, comments=[
            _claim_comment("dead-uuid", "abcd1234", _iso(seconds_ago=3600)),
        ])
        gh_cli_argv: list[tuple] = []

        def gh_cli_handler(*argv):
            gh_cli_argv.append(argv)
            r = mock.Mock()
            r.returncode = 0
            r.stdout = ""
            r.stderr = ""
            return r

        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(node),
        }, gh_cli_handler=gh_cli_handler):
            cli.reconcile_untracked_github_claims()

        # We should have removed the label and posted a release comment.
        verbs = [argv[:3] for argv in gh_cli_argv]
        self.assertIn(("issue", "edit", "42"), verbs)
        self.assertIn(("issue", "comment", "42"), verbs)

    def test_grace_period_skips_fresh_claim(self):
        # 30 seconds old < 60s grace period.
        node = _claim_node(42, comments=[
            _claim_comment("dead-uuid", "abcd1234", _iso(seconds_ago=30)),
        ])
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(node),
        }) as client:
            cli.reconcile_untracked_github_claims()
        # No GH writes — only the batch GraphQL.
        self.assertEqual(sum(1 for c in client.calls
                              if c["method"] == "GH"), 0)

    def test_already_tracked_issue_skipped(self):
        # If history already lists the issue, reconcile shouldn't touch it.
        node = _claim_node(42, comments=[
            _claim_comment("live-uuid", "abcd1234", _iso(seconds_ago=3600)),
        ])
        with mock.patch.object(cli, "load_claim_history", return_value={
            "42": {"session_uuid": "live-uuid", "short_id": "abcd1234"},
        }), patch_client(routes={
            ("POST", "/graphql"): self._gql_response(node),
        }) as client:
            cli.reconcile_untracked_github_claims()
        self.assertEqual(sum(1 for c in client.calls
                              if c["method"] == "GH"), 0)

    def test_label_missing_skipped(self):
        # If the GraphQL snapshot shows the label is already gone, skip.
        node = _claim_node(42, labels=("agent-plan",),  # no `claimed`
                            comments=[
            _claim_comment("dead-uuid", "abcd1234", _iso(seconds_ago=3600)),
        ])
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(node),
        }) as client:
            cli.reconcile_untracked_github_claims()
        self.assertEqual(sum(1 for c in client.calls
                              if c["method"] == "GH"), 0)


# ---------------------------------------------------------------------------
# B4 — provenance GraphQL
# ---------------------------------------------------------------------------

class ProvenanceBurnerTests(unittest.TestCase):
    def _gql_response(self, *, author="alice",
                       author_assoc="OWNER",
                       comments=(),
                       has_next=False):
        body = {"data": {"repository": {"issue": {
            "author": {"login": author},
            "authorAssociation": author_assoc,
            "comments": {
                "nodes": [
                    {"databaseId": cid,
                     "author": {"login": login},
                     "authorAssociation": assoc}
                    for cid, login, assoc in comments
                ],
                "pageInfo": {"hasNextPage": has_next,
                              "endCursor": "Y3Vyc29yOnY=" if has_next else None},
            },
        }}}}
        return fake_response(200, body=body)

    def test_owner_with_owner_comments(self):
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(
                author="alice", author_assoc="OWNER",
                comments=[(1, "alice", "OWNER"),
                          (2, "bob", "MEMBER")],
            ),
        }):
            prov = cli.fetch_issue_provenance("o/r", 42)
        self.assertEqual(prov.author_login, "alice")
        self.assertEqual(prov.author_association, "OWNER")
        self.assertEqual([(c.comment_id, c.login, c.association)
                          for c in prov.comments],
                         [(1, "alice", "OWNER"), (2, "bob", "MEMBER")])

    def test_404_raises_runtime_error(self):
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(
                200, body={"data": {"repository": {"issue": None}}}),
        }):
            with self.assertRaises(RuntimeError):
                cli.fetch_issue_provenance("o/r", 999)

    def test_5xx_raises_runtime_error(self):
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(
                500, body={"message": "server error"}),
        }):
            with self.assertRaises(RuntimeError):
                cli.fetch_issue_provenance("o/r", 42)

    def test_etag_cached_repeat_serves_304(self):
        """A repeat provenance fetch for the same issue must serve a
        304 from the layer's ETag cache rather than burning a fresh
        GraphQL call. This is the dominant GraphQL burner pre-fix —
        `cmd_filter_trusted_issues` runs as a fresh subprocess per
        `coordination orient` tick, so the in-process
        `_provenance_cache` doesn't deduplicate across invocations.
        The on-disk ETag cache does.
        """
        import httpx
        from pod import github as gh
        from pod.github import GitHubClient

        seen_if_none_match: list[str | None] = []

        def handler(req: httpx.Request) -> httpx.Response:
            seen_if_none_match.append(req.headers.get("if-none-match"))
            base_headers = {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": str(int(time.time()) + 3600),
                "x-ratelimit-resource": "graphql",
            }
            if req.headers.get("if-none-match") == '"prov-v1"':
                return httpx.Response(304, headers=base_headers)
            body = {"data": {"repository": {"issue": {
                "author": {"login": "alice"},
                "authorAssociation": "OWNER",
                "comments": {
                    "nodes": [{"databaseId": 1,
                                "author": {"login": "alice"},
                                "authorAssociation": "OWNER"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }}}}
            return httpx.Response(200, json=body,
                                   headers={**base_headers,
                                            "etag": '"prov-v1"'})

        with tempfile.TemporaryDirectory() as td:
            cache_dir = Path(td) / "gh-cache"
            log_path = Path(td) / "gh-access.log"
            client = GitHubClient(
                host="github.com",
                token="t-test",
                cache_dir=cache_dir,
                log_path=log_path,
                transport=httpx.MockTransport(handler),
                trim_cache_on_init=False,
                rate_cap_hz=0,
            )
            try:
                with mock.patch.object(gh, "get_client",
                                        return_value=client):
                    p1 = cli.fetch_issue_provenance("o/r", 42)
                    p2 = cli.fetch_issue_provenance("o/r", 42)
            finally:
                client.close()

        # First call had no If-None-Match (cold cache); second sent
        # the stored etag.
        self.assertEqual(seen_if_none_match, [None, '"prov-v1"'])
        # Both calls must return semantically identical results; the
        # 304-served second response uses the cached body.
        self.assertEqual(p1.author_login, p2.author_login)
        self.assertEqual(p1.author_association, p2.author_association)
        self.assertEqual([c.comment_id for c in p1.comments],
                         [c.comment_id for c in p2.comments])

    def test_more_than_100_comments_falls_back_to_rest(self):
        # First page (GraphQL) has 100 comments AND hasNextPage=True.
        gql_comments = [(cid, "alice", "MEMBER") for cid in range(1, 101)]
        gql_resp = self._gql_response(
            author="alice", author_assoc="OWNER",
            comments=gql_comments, has_next=True,
        )
        # REST follow-up returns one extra untrusted commenter at id 200.
        rest_resp = fake_response(200, body=[{
            "id": 200,
            "user": {"login": "stranger"},
            "author_association": "NONE",
        }])
        with patch_client(routes={
            ("POST", "/graphql"): gql_resp,
            ("GET", "/repos/o/r/issues/42/comments"): rest_resp,
        }):
            prov = cli.fetch_issue_provenance("o/r", 42)
        ids = {c.comment_id for c in prov.comments}
        self.assertIn(200, ids)
        # The new comment's association is untrusted (NONE).
        late = next(c for c in prov.comments if c.comment_id == 200)
        self.assertEqual(late.association, "NONE")
        self.assertEqual(late.login, "stranger")


if __name__ == "__main__":
    unittest.main()
