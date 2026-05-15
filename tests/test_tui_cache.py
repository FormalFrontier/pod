"""Tests for `.pod/tui-cache.json` persistence and the stale-render path
in `_tui_refresh_batch`.

The TUI's GraphQL batch refresh used to return `None` on any failure
(rate limit, transient 5xx, network error), which collapsed the items
panel to zero rows. The cache persists the last successful response
body to disk and the refresh helper falls back to it on the next failed
tick, with a freshness marker in the header so the operator knows
they're looking at history.
"""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import cli

from _gh_helpers import fake_response, patch_client


_VALID_REPO_NODE = {
    "openAgentPlan": {"nodes": []},
    "openDirective": {"nodes": []},
    "closedAgentPlan": {"nodes": []},
    "closedDirective": {"nodes": []},
    "blocked": {"nodes": []},
    "hasPrIssues": {"nodes": []},
    "pullRequests": {"nodes": []},
}


def _git_remote_mock(slug: str):
    return mock.Mock(returncode=0, stderr="",
                     stdout=f"git@github.com:{slug}.git\n")


class TuiCacheRoundTripTests(unittest.TestCase):
    """`_save_tui_cache` + `_load_tui_cache` exercised directly."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.path = Path(self._tmp.name) / "tui-cache.json"
        p = mock.patch.object(cli, "_tui_cache_path", return_value=self.path)
        p.start()
        self.addCleanup(p.stop)

    def test_round_trip(self):
        cli._save_tui_cache(123.0, "owner/repo", _VALID_REPO_NODE)
        loaded = cli._load_tui_cache("owner/repo")
        self.assertIsNotNone(loaded)
        ts, body = loaded
        self.assertEqual(ts, 123.0)
        self.assertEqual(body, _VALID_REPO_NODE)

    def test_missing_file_returns_none(self):
        self.assertIsNone(cli._load_tui_cache("owner/repo"))

    def test_corrupt_json_returns_none(self):
        self.path.write_text("{not valid json")
        self.assertIsNone(cli._load_tui_cache("owner/repo"))

    def test_schema_mismatch_rejected(self):
        self.path.write_text(json.dumps({
            "schema": cli._TUI_CACHE_SCHEMA + 1,
            "repo": "owner/repo",
            "fetched_at": 1.0,
            "body": _VALID_REPO_NODE,
        }))
        self.assertIsNone(cli._load_tui_cache("owner/repo"))

    def test_repo_mismatch_rejected(self):
        # Snapshot saved while pod was running against a different repo
        # (e.g. .pod/ copied between projects) must not feed back into
        # this one.
        cli._save_tui_cache(1.0, "other/elsewhere", _VALID_REPO_NODE)
        self.assertIsNone(cli._load_tui_cache("owner/repo"))

    def test_missing_required_keys_rejected(self):
        # Old-shape snapshot from a prior pod version where the GraphQL
        # query lacked, say, `hasPrIssues`. The schema-version bump is
        # the primary defence; this key-set check is belt-and-braces.
        body = dict(_VALID_REPO_NODE)
        body.pop("hasPrIssues")
        self.path.write_text(json.dumps({
            "schema": cli._TUI_CACHE_SCHEMA,
            "repo": "owner/repo",
            "fetched_at": 1.0,
            "body": body,
        }))
        self.assertIsNone(cli._load_tui_cache("owner/repo"))

    def test_save_is_chmod_600(self):
        cli._save_tui_cache(1.0, "owner/repo", _VALID_REPO_NODE)
        import os
        import stat
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        # Issue bodies and comments are in the payload; not world- or
        # group-readable.
        self.assertEqual(mode & 0o077, 0)


class TuiRefreshBatchTests(unittest.TestCase):
    """End-to-end refresh helper: live success, live failure with
    stale-from-disk fallback, schema-mismatch ignored."""

    def setUp(self):
        # Reset module state between tests.
        cli._TUI_REFRESH_CACHE = (0.0, None)
        cli._TUI_DISK_CACHE_LOADED = False
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.cache_path = Path(self._tmp.name) / "tui-cache.json"
        p = mock.patch.object(cli, "_tui_cache_path",
                              return_value=self.cache_path)
        p.start()
        self.addCleanup(p.stop)
        # Stub `_get_repo()` so we don't shell out to git.
        p2 = mock.patch.object(cli, "_get_repo", return_value="owner/repo")
        p2.start()
        self.addCleanup(p2.stop)

    def _gql_response(self, *, status=200, body=None):
        body = body if body is not None else {
            "data": {"repository": _VALID_REPO_NODE}
        }
        return fake_response(status, body=body)

    def test_successful_refresh_writes_disk_cache(self):
        with patch_client(routes={
            ("POST", "/graphql"): self._gql_response(),
        }):
            out = cli._tui_refresh_batch()
        self.assertEqual(out, _VALID_REPO_NODE)
        # Disk cache should be written.
        self.assertTrue(self.cache_path.exists())
        data = json.loads(self.cache_path.read_text())
        self.assertEqual(data["schema"], cli._TUI_CACHE_SCHEMA)
        self.assertEqual(data["repo"], "owner/repo")
        self.assertEqual(data["body"], _VALID_REPO_NODE)

    def test_stale_render_when_live_call_fails(self):
        # Seed disk cache, simulate cold pod start, simulate live
        # GraphQL failure (500).
        cli._save_tui_cache(time.time() - 600, "owner/repo",
                            _VALID_REPO_NODE)
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(500, body={"message": "x"}),
        }):
            out = cli._tui_refresh_batch()
        self.assertEqual(out, _VALID_REPO_NODE,
                         "stale snapshot should be returned on failure")

    def test_stale_render_when_graphql_rate_limited(self):
        # GraphQL returns 200 with empty data (RATE_LIMITED shape).
        cli._save_tui_cache(time.time() - 600, "owner/repo",
                            _VALID_REPO_NODE)
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body={
                "errors": [{"type": "RATE_LIMITED"}],
                "data": None,
            }),
        }):
            out = cli._tui_refresh_batch()
        self.assertEqual(out, _VALID_REPO_NODE)

    def test_disk_cache_loaded_once_per_process(self):
        # After the first cold-start tick, even if disk is hit again
        # (rewritten externally), the in-memory cache is not refreshed
        # from disk a second time.
        cli._save_tui_cache(100.0, "owner/repo", _VALID_REPO_NODE)
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(500, body={}),
        }):
            cli._tui_refresh_batch()  # first tick: loads disk cache
            self.assertTrue(cli._TUI_DISK_CACHE_LOADED)
            ts1, _ = cli._TUI_REFRESH_CACHE
        # Overwrite disk with a different ts; the next refresh tick
        # should not pick it up — we already had our one-shot.
        cli._save_tui_cache(200.0, "owner/repo", _VALID_REPO_NODE)
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(500, body={}),
        }):
            cli._tui_refresh_batch()
            ts2, _ = cli._TUI_REFRESH_CACHE
        self.assertEqual(ts1, ts2)

    def test_no_disk_cache_no_stale_fallback(self):
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(500, body={}),
        }):
            out = cli._tui_refresh_batch()
        self.assertIsNone(out)
        # In-memory cache must remain unset so subsequent successful
        # refreshes can populate it cleanly.
        ts, body = cli._TUI_REFRESH_CACHE
        self.assertIsNone(body)


if __name__ == "__main__":
    unittest.main()
