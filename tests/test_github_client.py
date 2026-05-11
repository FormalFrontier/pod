"""Tests for the GitHub access layer (pod/github.py).

Uses httpx.MockTransport so no network is touched. Each test constructs a
fresh client pointed at a tempdir for cache + log; the test asserts on
the cache file, log file, rate-meter state, and (where relevant) on the
sequence of MockTransport calls.
"""

from __future__ import annotations

import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import httpx

from pod.github import (
    DEFAULT_BACKPRESSURE_THRESHOLD,
    GHResponse,
    GitHubClient,
    RateSnapshot,
    _ETagStore,
    _parse_next_link,
    _redact_argv,
    _redact_url,
)


def _client(handler, *, cache_dir, log_path, **kw):
    return GitHubClient(
        host="github.com",
        token="t-test",
        cache_dir=cache_dir,
        log_path=log_path,
        transport=httpx.MockTransport(handler),
        # Disable cache trim during tests so we can plant entries directly.
        trim_cache_on_init=False,
        # Disable the soft per-process rate cap so tests don't sleep.
        rate_cap_hz=0,
        **kw,
    )


def _rl_headers(*, remaining=4999, limit=5000, reset=None,
                resource="core") -> dict:
    if reset is None:
        reset = int(time.time()) + 3600
    return {
        "x-ratelimit-limit": str(limit),
        "x-ratelimit-remaining": str(remaining),
        "x-ratelimit-reset": str(reset),
        "x-ratelimit-resource": resource,
    }


def _resp(status=200, *, json_body=None, headers=None, etag=None,
          link=None) -> httpx.Response:
    h = dict(_rl_headers())
    if headers:
        h.update(headers)
    if etag:
        h["etag"] = etag
    if link:
        h["link"] = link
    return httpx.Response(status, json=json_body, headers=h)


class _Tmp(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)
        self.cache_dir = self.tmp / "gh-cache"
        self.log_path = self.tmp / "gh-access.log"


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class HelperTests(unittest.TestCase):
    def test_redact_url_strips_secrets(self):
        u = "/foo?bar=1&access_token=SECRET&token=DEADBEEF"
        out = _redact_url(u)
        self.assertNotIn("SECRET", out)
        self.assertNotIn("DEADBEEF", out)
        self.assertIn("bar=1", out)
        self.assertIn("access_token=REDACTED", out)
        self.assertIn("token=REDACTED", out)

    def test_redact_url_passthrough_for_clean(self):
        self.assertEqual(_redact_url("/repos/o/r/issues/1"),
                         "/repos/o/r/issues/1")

    def test_redact_argv_redacts_authorization_header_value(self):
        argv = ("api", "/x", "-H", "Authorization: Bearer SECRETSECRET")
        out = _redact_argv(argv)
        self.assertEqual(out, ["api", "/x", "-H", "REDACTED"])

    def test_redact_argv_keeps_normal_args(self):
        argv = ("issue", "list", "--label", "agent-plan", "--state", "open")
        self.assertEqual(_redact_argv(argv), list(argv))

    def test_parse_next_link(self):
        h = ('<https://api.github.com/repos/o/r/issues?page=2>; rel="next", '
             '<https://api.github.com/repos/o/r/issues?page=5>; rel="last"')
        self.assertEqual(_parse_next_link(h),
                         "https://api.github.com/repos/o/r/issues?page=2")
        self.assertIsNone(_parse_next_link(""))
        self.assertIsNone(_parse_next_link('<x>; rel="last"'))

    def test_etag_key_includes_host_method_url_params_accept_apiversion(self):
        a = _ETagStore.key_for("github.com", "GET", "/x",
                                {"a": 1}, "application/vnd.github+json",
                                "2022-11-28")
        b = _ETagStore.key_for("github.com", "GET", "/x",
                                {"a": 2}, "application/vnd.github+json",
                                "2022-11-28")
        self.assertNotEqual(a, b)
        # Param order does not affect the key.
        c = _ETagStore.key_for("github.com", "GET", "/x",
                                {"a": 1, "b": 2},
                                "application/vnd.github+json",
                                "2022-11-28")
        d = _ETagStore.key_for("github.com", "GET", "/x",
                                {"b": 2, "a": 1},
                                "application/vnd.github+json",
                                "2022-11-28")
        self.assertEqual(c, d)


# ---------------------------------------------------------------------------
# Request semantics + ETag round-trip
# ---------------------------------------------------------------------------

class RequestTests(_Tmp):
    def test_get_returns_parsed_body(self):
        def h(req: httpx.Request) -> httpx.Response:
            return _resp(200, json_body={"hello": "world"}, etag='"v1"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            r = c.get("/repos/o/r/issues/1")
            self.assertTrue(r.ok())
            self.assertEqual(r.status, 200)
            self.assertEqual(r.body(), {"hello": "world"})
            self.assertFalse(r.cache_hit)
        finally:
            c.close()

    def test_etag_round_trip_serves_304_from_cache(self):
        seen = []

        def h(req: httpx.Request) -> httpx.Response:
            seen.append(req.headers.get("if-none-match"))
            if req.headers.get("if-none-match") == '"v1"':
                return httpx.Response(304, headers=_rl_headers(remaining=4998))
            return _resp(200, json_body={"x": 1}, etag='"v1"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            r1 = c.get("/repos/o/r/issues/1")
            self.assertEqual(r1.status, 200)
            r2 = c.get("/repos/o/r/issues/1")
            self.assertEqual(r2.status, 304)
            self.assertTrue(r2.cache_hit)
            self.assertEqual(r2.body(), {"x": 1})  # served from cache
        finally:
            c.close()

        # First call had no If-None-Match; second call sent "v1".
        self.assertEqual(seen, [None, '"v1"'])

    def test_writes_bypass_etag_cache(self):
        calls = []

        def h(req: httpx.Request) -> httpx.Response:
            calls.append(req.method)
            return _resp(201, json_body={"ok": True}, etag='"x"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.post("/repos/o/r/issues/1/comments", json={"body": "hi"})
            c.post("/repos/o/r/issues/1/comments", json={"body": "hi"})
        finally:
            c.close()
        self.assertEqual(calls, ["POST", "POST"])
        self.assertFalse(self.cache_dir.exists() and any(
            self.cache_dir.iterdir()))

    def test_cache_file_is_chmod_600(self):
        def h(req): return _resp(200, json_body={"x": 1}, etag='"v"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/x")
        finally:
            c.close()
        files = list(self.cache_dir.iterdir())
        self.assertEqual(len(files), 1)
        m = files[0].stat().st_mode
        # Owner rw, no group/other access.
        self.assertEqual(stat.S_IMODE(m) & 0o077, 0,
                         f"world/group bits set: {oct(m)}")


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------

class PaginateTests(_Tmp):
    def test_walks_link_header(self):
        # Identify a request by its `page` query param (default = page 1).
        page_to_next = {1: 2, 2: 3, 3: None}

        def h(req: httpx.Request) -> httpx.Response:
            page_n = int(req.url.params.get("page", "1"))
            next_n = page_to_next[page_n]
            link = (f'<https://api.github.com/r/o/issues?page={next_n}>; '
                    f'rel="next"') if next_n else None
            return _resp(200, json_body=[{"page": page_n}], link=link)

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            seen_pages = []
            for r in c.paginate("/r/o/issues", per_page=10):
                self.assertTrue(r.ok())
                seen_pages.append(r.body()[0]["page"])
        finally:
            c.close()
        self.assertEqual(seen_pages, [1, 2, 3])

    def test_list_open_issues_excludes_prs_by_default(self):
        # REST /issues returns PRs as issue-shaped objects with
        # `pull_request` set. `gh issue list` excludes them. Our helper
        # must match `gh`'s sense.
        def h(req: httpx.Request) -> httpx.Response:
            body = [
                {"number": 1, "title": "real issue"},
                {"number": 2, "title": "actually a PR",
                 "pull_request": {"url": "..."}},
                {"number": 3, "title": "another issue"},
            ]
            return _resp(200, json_body=body)

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            result = c.list_open_issues("o/r", labels=["agent-plan"])
        finally:
            c.close()
        self.assertEqual([x["number"] for x in result], [1, 3])

    def test_list_open_issues_include_prs_when_requested(self):
        def h(req: httpx.Request) -> httpx.Response:
            return _resp(200, json_body=[
                {"number": 1, "title": "issue"},
                {"number": 2, "title": "pr", "pull_request": {"url": "..."}},
            ])

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            result = c.list_open_issues("o/r", exclude_prs=False)
        finally:
            c.close()
        self.assertEqual([x["number"] for x in result], [1, 2])

    def test_per_page_in_cache_key(self):
        # Same URL with different per_page should miss each other's cache.
        def h(req: httpx.Request) -> httpx.Response:
            pp = req.url.params.get("per_page", "?")
            return _resp(200, json_body=[{"pp": pp}], etag=f'"v-{pp}"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            list(c.paginate("/r/o/issues", per_page=10, max_pages=1))
            list(c.paginate("/r/o/issues", per_page=100, max_pages=1))
        finally:
            c.close()
        files = list(self.cache_dir.iterdir())
        self.assertEqual(len(files), 2)


# ---------------------------------------------------------------------------
# Rate meter + back-pressure
# ---------------------------------------------------------------------------

class RateMeterTests(_Tmp):
    def test_rate_meter_updates_from_headers(self):
        def h(req): return _resp(200, json_body={},
                                  headers=_rl_headers(remaining=4321))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/x")
            self.assertEqual(c.rate()["core"].remaining, 4321)
        finally:
            c.close()

    def test_graphql_uses_graphql_bucket(self):
        def h(req):
            return _resp(200, json_body={"data": {}},
                         headers=_rl_headers(remaining=4500,
                                             resource="graphql"))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.graphql("query { viewer { login } }")
            self.assertEqual(c.rate()["graphql"].remaining, 4500)
        finally:
            c.close()

    def test_graphql_post_never_sends_if_none_match(self):
        """GitHub's GraphQL endpoint does not honor conditional
        requests — field data showed zero 304s out of ~14k POSTs over
        four hours, with every POST debited from the GraphQL bucket.
        The layer therefore must not send `If-None-Match` on `/graphql`
        even when the caller passes `cache="etag"`, and must not
        persist the response in the ETag store."""
        seen_if_none_match: list[str | None] = []

        def h(req: httpx.Request) -> httpx.Response:
            seen_if_none_match.append(req.headers.get("if-none-match"))
            return _resp(200,
                          json_body={"data": {"repository": {"x": 1}}},
                          etag='"v1"',
                          headers={"x-ratelimit-resource": "graphql"})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.graphql("query Q { repository { x } }", cache="etag")
            c.graphql("query Q { repository { x } }", cache="etag")
        finally:
            c.close()
        self.assertEqual(seen_if_none_match, [None, None])
        # Cache dir should be empty (or absent) — no GraphQL entry.
        self.assertFalse(self.cache_dir.exists() and any(
            self.cache_dir.iterdir()))

    def test_graphql_default_cache_none_not_cached(self):
        """`graphql()` defaults to cache='none' so plain calls go
        uncached. (Callers that want caching must opt in via
        `cache="etag"`.)"""
        def h(req: httpx.Request) -> httpx.Response:
            return _resp(200, json_body={"data": {}}, etag='"v1"',
                          headers={"x-ratelimit-resource": "graphql"})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.graphql("query { viewer { login } }")  # default cache=none
        finally:
            c.close()
        self.assertFalse(self.cache_dir.exists() and any(
            self.cache_dir.iterdir()))

    def test_backpressure_sleeps_when_remaining_low(self):
        def h(req):
            return _resp(200, json_body={},
                         headers=_rl_headers(
                             remaining=DEFAULT_BACKPRESSURE_THRESHOLD - 1,
                             reset=int(time.time()) + 5))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            # First call seeds the rate meter.
            c.get("/x")
            slept: list[float] = []
            with mock.patch("pod.github.time.sleep",
                            side_effect=slept.append):
                c.get("/y")  # should trigger sleep before this call
            self.assertTrue(slept, "expected back-pressure sleep")
            self.assertGreater(slept[0], 0)
            self.assertLessEqual(slept[0], 60)
        finally:
            c.close()

    def test_retry_after_on_403_then_retry(self):
        calls = [0]

        def h(req):
            calls[0] += 1
            if calls[0] == 1:
                return httpx.Response(403, headers={**_rl_headers(),
                                                     "retry-after": "1"},
                                       json={"message": "rate-limited"})
            return _resp(200, json_body={"ok": True})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            with mock.patch("pod.github.time.sleep") as slept:
                r = c.get("/x")
            self.assertEqual(r.status, 200)
            self.assertEqual(calls[0], 2)
            slept.assert_any_call(1.0)
        finally:
            c.close()


# ---------------------------------------------------------------------------
# Auth refresh on 401
# ---------------------------------------------------------------------------

class AuthRefreshTests(_Tmp):
    def test_401_triggers_token_refresh_and_retry(self):
        bearers_seen = []

        def h(req):
            bearers_seen.append(req.headers.get("authorization"))
            if len(bearers_seen) == 1:
                return httpx.Response(401, json={"message": "Bad credentials"},
                                       headers=_rl_headers())
            return _resp(200, json_body={"ok": True})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            with mock.patch("pod.github._gh_token", return_value="t-fresh"):
                r = c.get("/x")
            self.assertEqual(r.status, 200)
        finally:
            c.close()
        self.assertEqual(bearers_seen,
                         ["Bearer t-test", "Bearer t-fresh"])

    def test_401_without_token_change_does_not_loop(self):
        def h(req):
            return httpx.Response(401, json={"message": "Bad credentials"},
                                   headers=_rl_headers())

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            with mock.patch("pod.github._gh_token", return_value="t-test"):
                r = c.get("/x")
            self.assertEqual(r.status, 401)  # surface, no infinite retry
        finally:
            c.close()


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class LoggingTests(_Tmp):
    def _read_log(self) -> list[dict]:
        if not self.log_path.exists():
            return []
        return [json.loads(l) for l in self.log_path.read_text().splitlines()
                if l.strip()]

    def test_request_writes_log_row(self):
        def h(req): return _resp(200, json_body={"x": 1}, etag='"v"')

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/repos/o/r/issues/1")
        finally:
            c.close()
        rows = self._read_log()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["verb"], "GET")
        self.assertEqual(row["url"], "/repos/o/r/issues/1")
        self.assertEqual(row["status"], 200)
        self.assertEqual(row["bucket"], "core")
        self.assertEqual(row["transport"], "httpx")
        self.assertIn("ts", row)
        self.assertIn("ms", row)
        self.assertIn("caller", row)

    def test_log_redacts_sensitive_query_params(self):
        def h(req): return _resp(200, json_body={})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/x", params={"access_token": "SECRET", "ok": "1"})
        finally:
            c.close()
        rows = self._read_log()
        self.assertEqual(len(rows), 1)
        self.assertNotIn("SECRET", rows[0]["url"])
        self.assertIn("REDACTED", rows[0]["url"])

    def test_log_does_not_contain_authorization_header(self):
        def h(req): return _resp(200, json_body={})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/x")
        finally:
            c.close()
        text = self.log_path.read_text()
        self.assertNotIn("Bearer", text)
        self.assertNotIn("t-test", text)

    def test_log_is_chmod_600(self):
        def h(req): return _resp(200, json_body={})

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            c.get("/x")
        finally:
            c.close()
        m = self.log_path.stat().st_mode
        self.assertEqual(stat.S_IMODE(m) & 0o077, 0,
                         f"world/group bits set: {oct(m)}")

    def test_gh_cli_logs_with_gh_transport(self):
        def h(req):
            raise AssertionError("gh_cli should not hit httpx")

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        # Mock subprocess.run so we don't actually invoke gh.
        completed = mock.Mock(returncode=0, stdout=b"", stderr=b"")
        with mock.patch("pod.github.subprocess.run", return_value=completed):
            c.gh_cli("issue", "list", "--label", "agent-plan")
        c.close()
        rows = self._read_log()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["transport"], "gh")
        self.assertEqual(rows[0]["verb"], "GH")
        self.assertIn("issue", rows[0]["url"])


# ---------------------------------------------------------------------------
# Cache trim
# ---------------------------------------------------------------------------

class CacheTrimTests(_Tmp):
    def test_trim_drops_old_entries(self):
        store = _ETagStore(self.cache_dir)
        store.save("a", etag='"x"', body={}, status=200, url="/x", headers={})
        store.save("b", etag='"x"', body={}, status=200, url="/x", headers={})
        # Age 'a' beyond the 7-day cutoff.
        a_path = self.cache_dir / "a.json"
        old = time.time() - (8 * 86400)
        os.utime(a_path, (old, old))
        store.trim()
        self.assertFalse(a_path.exists())
        self.assertTrue((self.cache_dir / "b.json").exists())


class LowWaterWarningTests(_Tmp):
    """`_maybe_warn_quota_low` fires once per reset window when a bucket
    dips below `_QUOTA_LOW_THRESHOLD`, and includes the top callers from
    the rolling buffer (covering both `request()` and `gh_cli()` paths).
    Suppressed when the reset is imminent so bursty callers near reset
    don't get nagged."""

    def _capture_stderr(self):
        # Patch `sys.stderr.write` at the module level so writes from
        # `_maybe_warn_quota_low` land in our list.
        from pod import github as gh_mod
        chunks: list[str] = []
        return mock.patch.object(gh_mod.sys.stderr, "write",
                                 side_effect=chunks.append), chunks

    def test_warning_fires_once_per_reset_window(self):
        reset = int(time.time()) + 1800  # 30 min away, well past time-gate

        def h(req):
            return _resp(200, json_body={},
                         headers=_rl_headers(remaining=10, reset=reset))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            patcher, chunks = self._capture_stderr()
            # Below `backpressure_threshold`=50, the layer would sleep
            # for real between calls. Mute it so the test runs fast.
            with patcher, mock.patch("pod.github.time.sleep"):
                c.get("/a")
                c.get("/b")
                c.get("/c")
            # Concatenate emitted stderr writes; should contain exactly
            # one warning despite three low-bucket responses.
            blob = "".join(chunks)
            self.assertEqual(blob.count("pod: gh-quota: core bucket low"), 1,
                             f"expected exactly one warning, got: {blob!r}")
        finally:
            c.close()

    def test_warning_skipped_when_reset_is_imminent(self):
        # Reset in 30 seconds — under the 120s time-gate.
        reset = int(time.time()) + 30

        def h(req):
            return _resp(200, json_body={},
                         headers=_rl_headers(remaining=5, reset=reset))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            patcher, chunks = self._capture_stderr()
            with patcher:
                c.get("/a")
            self.assertEqual("".join(chunks), "",
                             "warning should be suppressed near reset")
        finally:
            c.close()

    def test_warning_lists_top_callers(self):
        reset = int(time.time()) + 1800

        def h(req):
            return _resp(200, json_body={},
                         headers=_rl_headers(remaining=50, reset=reset))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            # Seed the buffer with several "callers" by stuffing the
            # deque directly — easier than constructing real stack
            # frames per caller identity.
            now = time.time()
            with c._lock:
                c._recent_calls.extend([
                    (now, "cli.py:fetch_issues_and_prs:1", "core", "httpx"),
                    (now, "cli.py:fetch_issues_and_prs:1", "core", "httpx"),
                    (now, "cli.py:fetch_issues_and_prs:1", "core", "httpx"),
                    (now, "cli.py:sync_claims:42", "core", "httpx"),
                    (now, "cli.py:sync_claims:42", "core", "httpx"),
                    (now, "cli.py:create_pr:99", "core", "gh"),
                ])
            patcher, chunks = self._capture_stderr()
            # Remaining=50 is below backpressure_threshold=50 — the
            # second call would block on a real sleep without this mute.
            with patcher, mock.patch("pod.github.time.sleep"):
                c.get("/trigger-warning")  # forces _maybe_warn_quota_low
            blob = "".join(chunks)
            self.assertIn("recent callers", blob)
            self.assertIn("fetch_issues_and_prs", blob)
            # gh_cli entries are tagged so the user knows they are
            # subprocess-shape, not direct HTTP.
            self.assertIn("(gh-cli)", blob)
        finally:
            c.close()

    def test_gh_cli_path_records_in_buffer(self):
        # `gh_cli` shells out via subprocess — verify the call is logged
        # into the rolling buffer so the warning surfaces PR-heavy
        # workflows that don't go through `request()`.
        def h(req):
            return _resp(200, json_body={}, headers=_rl_headers())

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            with mock.patch("pod.github.subprocess.run") as run:
                run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
                c.gh_cli("pr", "view", "42")
            self.assertEqual(len(c._recent_calls), 1)
            ts, caller, bucket, transport = c._recent_calls[0]
            self.assertEqual(bucket, "core")
            self.assertEqual(transport, "gh")
        finally:
            c.close()

    def test_low_water_buckets_clears_after_reset(self):
        reset_soon = int(time.time()) + 300  # 5 min away, past time-gate

        def h(req):
            return _resp(200, json_body={},
                         headers=_rl_headers(remaining=10, reset=reset_soon))

        c = _client(h, cache_dir=self.cache_dir, log_path=self.log_path)
        try:
            patcher, _ = self._capture_stderr()
            with patcher, mock.patch("pod.github.time.sleep"):
                c.get("/x")
            # Bucket is in the low state.
            self.assertEqual(c.low_water_buckets(), {"core"})
            # Manually age out the warn state.
            with c._lock:
                c._quota_warn_state["core"] = time.time() - 10
            self.assertEqual(c.low_water_buckets(), set())
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
