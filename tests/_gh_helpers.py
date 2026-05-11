"""Shared helpers for tests that need to fake the layer client.

Used by `test_release_claim_idempotent.py`, `test_security.py`, and any
other test that previously mocked `cli.subprocess.run` for `gh api …`
calls. After the github-access layer migration, those calls go through
`pod.github.GitHubClient` (httpx) — so tests need to mock the client
directly. The remaining `gh_cli(...)` porcelain calls still go through
`subprocess.run`, so most tests keep their `mock.patch.object(cli.subprocess,
"run", side_effect=fake_run)` for those and additionally use the
helpers here to fake the client's HTTP calls.
"""

from __future__ import annotations

import contextlib
from typing import Any, Iterable, Iterator
from unittest import mock

from pod import github as gh
from pod.github import GHResponse, RateSnapshot


def fake_response(status: int = 200, *, body: Any = None,
                  headers: dict | None = None,
                  cache_hit: bool = False,
                  body_cached: Any = None) -> GHResponse:
    """Build a `GHResponse` for a fake HTTP call."""
    return GHResponse(
        status=status,
        json=body,
        body_cached=body_cached if body_cached is not None else body,
        headers=headers or {},
        ms=1.0,
        cache_hit=cache_hit,
        rate=RateSnapshot(bucket="core", limit=5000, remaining=4999),
    )


class _FakeClient:
    """A drop-in for `pod.github.GitHubClient` in tests.

    Pass an optional `routes` dict mapping `(METHOD, PATH)` tuples to
    `GHResponse` (or a list-of-GHResponse for sequential calls). Anything
    not in `routes` falls back to `default` (default: 404). Records every
    call in `self.calls` so tests can assert on URL / params / method.
    """

    def __init__(self, *, routes: dict | None = None,
                 default: GHResponse | None = None,
                 gh_cli_handler=None):
        self.routes = dict(routes or {})
        self.default = default or fake_response(404, body={"message": "Not Found"})
        self.calls: list[dict] = []
        # Optional: callable taking (*argv) and returning a CompletedProcess-
        # shaped Mock. If None, gh_cli returns a 0/empty Mock by default.
        self.gh_cli_handler = gh_cli_handler

    def _serve(self, method: str, path: str,
               *, params=None, json=None, **kw) -> GHResponse:
        self.calls.append({"method": method, "path": path,
                           "params": params, "json": json})
        key = (method, path)
        v = self.routes.get(key, self.default)
        if isinstance(v, list):
            if not v:
                return self.default
            return v.pop(0)
        return v

    def get(self, path, *, params=None, **kw):
        return self._serve("GET", path, params=params, **kw)

    def post(self, path, *, json=None, **kw):
        return self._serve("POST", path, json=json, **kw)

    def put(self, path, *, json=None, **kw):
        return self._serve("PUT", path, json=json, **kw)

    def patch(self, path, *, json=None, **kw):
        return self._serve("PATCH", path, json=json, **kw)

    def delete(self, path, **kw):
        return self._serve("DELETE", path, **kw)

    def graphql(self, query, variables=None, **kw):
        return self._serve("POST", "/graphql",
                           json={"query": query, "variables": variables})

    def paginate(self, path, *, params=None, per_page=100,
                 max_pages=None, cache="etag") -> Iterator[GHResponse]:
        # For simplicity in unit tests, treat paginate as a single page hit.
        yield self._serve("GET", path, params=params)

    def gh_cli(self, *argv, **kw):
        self.calls.append({"method": "GH", "argv": argv})
        if self.gh_cli_handler is not None:
            return self.gh_cli_handler(*argv)
        result = mock.Mock()
        result.returncode = 0
        result.stdout = ""
        result.stderr = ""
        return result

    def rate(self):
        return {"core": RateSnapshot(bucket="core", limit=5000, remaining=4999),
                "graphql": RateSnapshot(bucket="graphql", limit=5000, remaining=4999)}


@contextlib.contextmanager
def patch_client(client: _FakeClient | None = None,
                 *, routes: dict | None = None,
                 default: GHResponse | None = None,
                 gh_cli_handler=None):
    """Context manager that swaps `gh.get_client()` to return `client`
    (or a fresh `_FakeClient` built from `routes` / `default`). Yields
    the active client so the test can inspect `.calls` afterward.

    `gh_cli_handler`, when provided, is a function `(*argv) ->
    CompletedProcess` invoked when production code calls `client.gh_cli(...)`.
    """
    if client is None:
        client = _FakeClient(routes=routes, default=default,
                              gh_cli_handler=gh_cli_handler)
    p = mock.patch.object(gh, "get_client", return_value=client)
    p.start()
    try:
        yield client
    finally:
        p.stop()
