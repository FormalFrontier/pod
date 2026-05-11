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
    def setUp(self):
        # Disk provenance cache leaks across tests (and from real pod
        # runs in the cwd), so redirect it to a tempdir per test.
        cli._provenance_cache.clear()
        self._cache_tmp = tempfile.TemporaryDirectory()
        self._disk_patch = mock.patch.object(
            cli, "_PROVENANCE_DISK_CACHE",
            cli._ProvenanceDiskCache(Path(self._cache_tmp.name)))
        self._disk_patch.start()

    def tearDown(self):
        self._disk_patch.stop()
        self._cache_tmp.cleanup()

    def _gql_response(self, *, author="alice",
                       author_assoc="OWNER",
                       comments=(),
                       has_next=False,
                       alias="i0"):
        # The batched provenance query returns aliased nodes (i0, i1,
        # ...) instead of `issue`. Single-issue fetches use alias `i0`.
        body = {"data": {"repository": {alias: {
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

    def test_disk_cache_short_circuits_repeat_fetch(self):
        """A repeat `check_issue_provenance` for the same issue inside
        the TTL must serve from the disk cache instead of issuing
        another GraphQL POST. This is what keeps the GraphQL bucket
        alive across the fresh subprocesses that `coordination orient`
        ticks spawn — GitHub does not honor `If-None-Match` on
        `/graphql` POSTs, so the layer-level ETag store can't help.
        """
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(
                author="alice", author_assoc="OWNER",
                comments=[(1, "alice", "OWNER")],
            ),
        }) as client, mock.patch.object(cli, "_is_repo_public",
                                          return_value=True):
            ok1, _ = cli.check_issue_provenance("o/r", 42, {})
            graphql_after_first = sum(
                1 for c in client.calls
                if c["method"] == "POST" and c["path"] == "/graphql")
            # Drop the in-process cache so we exercise the disk tier
            # (this is what a fresh subprocess would see).
            cli._provenance_cache.clear()
            ok2, _ = cli.check_issue_provenance("o/r", 42, {})
            graphql_after_second = sum(
                1 for c in client.calls
                if c["method"] == "POST" and c["path"] == "/graphql")
        self.assertTrue(ok1)
        self.assertTrue(ok2)
        self.assertEqual(graphql_after_first, 1)
        self.assertEqual(graphql_after_second, 1,
                          "second fetch should hit the disk cache, "
                          "not issue another GraphQL POST")

    def test_batched_fetch_issues_one_graphql_per_chunk(self):
        """`fetch_issue_provenances([n1, n2, ...])` must issue one
        GraphQL POST per chunk of `_PROVENANCE_BATCH_SIZE` issues —
        not one POST per issue."""
        # Build a batched response with aliased issue nodes for the
        # three requested issues.
        body = {"data": {"repository": {
            "i0": {
                "author": {"login": "alice"},
                "authorAssociation": "OWNER",
                "comments": {"nodes": [],
                              "pageInfo": {"hasNextPage": False,
                                            "endCursor": None}},
            },
            "i1": {
                "author": {"login": "bob"},
                "authorAssociation": "MEMBER",
                "comments": {"nodes": [],
                              "pageInfo": {"hasNextPage": False,
                                            "endCursor": None}},
            },
            "i2": {
                "author": {"login": "carol"},
                "authorAssociation": "COLLABORATOR",
                "comments": {"nodes": [],
                              "pageInfo": {"hasNextPage": False,
                                            "endCursor": None}},
            },
        }}}
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body=body),
        }) as client:
            got = cli.fetch_issue_provenances("o/r", [10, 20, 30])
        graphql_calls = [c for c in client.calls
                          if c["method"] == "POST"
                          and c["path"] == "/graphql"]
        self.assertEqual(len(graphql_calls), 1)
        self.assertEqual(set(got.keys()), {10, 20, 30})
        self.assertEqual(got[10].author_login, "alice")
        self.assertEqual(got[20].author_login, "bob")
        self.assertEqual(got[30].author_login, "carol")

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
