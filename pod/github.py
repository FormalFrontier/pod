"""GitHub API access layer for pod.

Single chokepoint for every GitHub interaction. Owns:
  - One `httpx.Client` per process, with bearer auth from `gh auth token`.
  - On-disk ETag store at .pod/gh-cache/, so repeated reads of unchanged
    resources serve a 304 (which doesn't count against the primary 5000/hr
    REST budget).
  - Rate meter populated from response headers (X-RateLimit-*); used for
    back-pressure (sleep until reset when remaining < threshold) plus a
    soft per-process request-rate cap (defence against secondary limits).
  - Per-call JSONL log at .pod/gh-access.log, redacting Authorization /
    sensitive query params.
  - Subprocess passthrough `gh_cli(*argv)` for porcelain commands that
    aren't 1:1 with REST/GraphQL (gh pr merge --auto, gh pr create, etc.);
    those are still logged + back-pressured.

This module replaces ad-hoc `subprocess.run(["gh", "api", ...])` scattered
through cli.py and the legacy `pod/data/coordination` bash script.
"""

from __future__ import annotations

import datetime
import hashlib
import inspect
import json
import collections
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Literal

import httpx


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_TIMEOUT = 30.0
DEFAULT_PER_PAGE = 100
DEFAULT_API_VERSION = "2022-11-28"
DEFAULT_ACCEPT = "application/vnd.github+json"

# Sleep until reset when fewer than this many requests remain in a bucket.
DEFAULT_BACKPRESSURE_THRESHOLD = 50
# Cap on a single back-pressure sleep; beyond this we surface to the caller.
MAX_BACKPRESSURE_SLEEP = 60.0
# Soft per-process request-rate cap (secondary-limit defence; still applies
# even on 304 cache hits, since GitHub does count those for some secondary
# limits).
DEFAULT_RATE_CAP_HZ = 50.0

CACHE_TRIM_AGE_DAYS = 7
CACHE_TRIM_MAX_ENTRIES = 5_000

LOG_ROTATE_LINES = 200_000


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class RateSnapshot:
    """Point-in-time view of a single rate-limit bucket."""
    bucket: str = "core"   # "core" or "graphql"
    limit: int = 0
    remaining: int = 0
    reset_at: float = 0.0  # unix timestamp
    observed_at: float = 0.0


@dataclass
class GHResponse:
    status: int
    json: Any = None
    body_cached: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    ms: float = 0.0
    cache_hit: bool = False
    rate: RateSnapshot = field(default_factory=RateSnapshot)
    raw: bytes = b""
    next_url: str | None = None  # /repos/.../issues?page=2 from Link header

    def ok(self) -> bool:
        return 200 <= self.status < 300 or self.status == 304

    def body(self) -> Any:
        """Returns the live JSON body, or the cached body for a 304."""
        return self.body_cached if self.cache_hit else self.json


# ---------------------------------------------------------------------------
# ETag store
# ---------------------------------------------------------------------------

class _ETagStore:
    """Per-URL ETag + body cache on disk.

    Cache key incorporates host, method, URL, normalised query params,
    Accept media type, and X-GitHub-Api-Version so that representation-
    affecting differences don't collide. Each cache file is chmod 600.
    """

    def __init__(self, root: Path):
        self.root = root
        self._lock = threading.Lock()

    def _path_for(self, key: str) -> Path:
        return self.root / f"{key}.json"

    @staticmethod
    def key_for(host: str, method: str, url: str, params: dict | None,
                accept: str, api_version: str,
                body: Any = None) -> str:
        """Compute a deterministic cache key. `body` is hashed into the
        key when present so POST `/graphql` requests (where the query
        lives in the body, not the URL) get distinct cache entries per
        distinct query+variables."""
        norm_params = ""
        if params:
            norm_params = urllib.parse.urlencode(
                sorted((k, v) for k, v in params.items()), doseq=True)
        body_hash = ""
        if body is not None:
            try:
                body_hash = hashlib.sha256(
                    json.dumps(body, sort_keys=True,
                               separators=(",", ":")).encode()
                ).hexdigest()
            except (TypeError, ValueError):
                body_hash = ""
        material = (f"{host}|{method}|{url}|{norm_params}|{accept}"
                    f"|{api_version}|{body_hash}")
        return hashlib.sha256(material.encode()).hexdigest()

    def load(self, key: str) -> dict | None:
        p = self._path_for(key)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    def save(self, key: str, *, etag: str, body: Any, status: int, url: str,
             headers: dict[str, str]) -> None:
        with self._lock:
            try:
                self.root.mkdir(parents=True, exist_ok=True)
            except OSError:
                return
            data = {
                "etag": etag,
                "body": body,
                "status": status,
                "fetched_at": time.time(),
                "url": url,
                # Only retain a few headers that affect representation; the
                # rest are noise and may include rate-limit data.
                "headers": {k.lower(): v for k, v in headers.items()
                            if k.lower() in ("content-type",
                                             "x-github-media-type")},
            }
            p = self._path_for(key)
            tmp = p.with_suffix(".tmp")
            try:
                tmp.write_text(json.dumps(data))
                os.chmod(tmp, 0o600)
                tmp.replace(p)
            except OSError:
                pass

    def trim(self) -> None:
        """Drop entries older than CACHE_TRIM_AGE_DAYS; if more than
        CACHE_TRIM_MAX_ENTRIES remain, keep the newest."""
        if not self.root.is_dir():
            return
        cutoff = time.time() - CACHE_TRIM_AGE_DAYS * 86400
        entries: list[tuple[float, Path]] = []
        for p in self.root.iterdir():
            if not p.is_file() or p.suffix != ".json":
                continue
            try:
                mtime = p.stat().st_mtime
            except OSError:
                continue
            if mtime < cutoff:
                try:
                    p.unlink()
                except OSError:
                    pass
                continue
            entries.append((mtime, p))
        if len(entries) > CACHE_TRIM_MAX_ENTRIES:
            entries.sort(reverse=True)
            for _, p in entries[CACHE_TRIM_MAX_ENTRIES:]:
                try:
                    p.unlink()
                except OSError:
                    pass


# ---------------------------------------------------------------------------
# Access log
# ---------------------------------------------------------------------------

_REDACT_QS_PARAMS = {"access_token", "client_secret", "auth", "token"}


def _redact_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    if not parts.query:
        return url
    qs = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    redacted = [(k, "REDACTED" if k.lower() in _REDACT_QS_PARAMS else v)
                for k, v in qs]
    return urllib.parse.urlunsplit(
        parts._replace(query=urllib.parse.urlencode(redacted)))


def _redact_argv(argv: tuple[str, ...] | list[str]) -> list[str]:
    """Redact `Authorization:` header values and `token …` patterns from
    a gh argv before logging. Defence in depth — pod doesn't pass auth
    on gh argv, but a future caller might."""
    out: list[str] = []
    redact_next = False
    for a in argv:
        if redact_next:
            out.append("REDACTED")
            redact_next = False
            continue
        if a in ("-H", "--header"):
            out.append(a)
            redact_next = True
            continue
        if "authorization:" in a.lower() or a.lower().startswith("token "):
            out.append("REDACTED")
            continue
        out.append(a)
    return out


class _AccessLog:
    """JSONL logger; one row per GitHub interaction. Thread-safe."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        self._fh: Any = None

    def _ensure(self) -> None:
        if self._fh is not None:
            return
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            return
        self._maybe_rotate()
        try:
            self._fh = open(self.path, "a", buffering=1)
            os.chmod(self.path, 0o600)
        except OSError:
            self._fh = None

    def _maybe_rotate(self) -> None:
        try:
            if not self.path.exists():
                return
            with open(self.path) as f:
                lines = sum(1 for _ in f)
            if lines >= LOG_ROTATE_LINES:
                rotated = self.path.with_suffix(self.path.suffix + ".1")
                if rotated.exists():
                    rotated.unlink()
                self.path.rename(rotated)
        except OSError:
            pass

    def write(self, **fields: Any) -> None:
        with self._lock:
            self._ensure()
            if self._fh is None:
                return
            try:
                # Drop None-valued fields to keep rows compact.
                row = {k: v for k, v in fields.items() if v is not None}
                self._fh.write(json.dumps(row, separators=(",", ":")) + "\n")
                self._fh.flush()
            except OSError:
                pass

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bucket_for_path(url: str) -> str:
    """Which rate-limit bucket a request consumes."""
    return "graphql" if url.endswith("/graphql") else "core"


_LINK_NEXT_RE = re.compile(r'<([^>]+)>;\s*rel="next"')


def _parse_next_link(link_header: str) -> str | None:
    if not link_header:
        return None
    m = _LINK_NEXT_RE.search(link_header)
    return m.group(1) if m else None


def _iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _iso_at(unix: float) -> str | None:
    if not unix:
        return None
    return datetime.datetime.fromtimestamp(unix, datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _gh_token(host: str = "github.com") -> str:
    """Read the bearer for `host` from `gh`. Raises RuntimeError on failure."""
    r = subprocess.run(
        ["gh", "auth", "token", "--hostname", host],
        capture_output=True, text=True, timeout=10)
    if r.returncode != 0:
        raise RuntimeError(
            f"gh auth token --hostname {host} failed: {r.stderr.strip()}")
    return r.stdout.strip()


def _default_cache_dir() -> Path:
    from pod import cli  # late import to avoid circular dependency
    return cli.POD_DIR / "gh-cache"


def _default_log_path() -> Path:
    from pod import cli
    return cli.POD_DIR / "gh-access.log"


def _default_user_agent() -> str:
    from pod import __version__
    return f"pod/{__version__} (https://github.com/FormalFrontier/pod)"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class GitHubClient:
    """Single chokepoint for pod's GitHub interactions."""

    def __init__(self, *,
                 host: str = "github.com",
                 token: str | None = None,
                 cache_dir: Path | None = None,
                 log_path: Path | None = None,
                 backpressure_threshold: int = DEFAULT_BACKPRESSURE_THRESHOLD,
                 rate_cap_hz: float = DEFAULT_RATE_CAP_HZ,
                 transport: httpx.BaseTransport | None = None,
                 user_agent: str | None = None,
                 trim_cache_on_init: bool = True):
        self.host = host
        try:
            self._token = token if token is not None else _gh_token(host)
        except RuntimeError:
            # Defer the failure until the first request, so test harnesses
            # and short-lived CLI commands that never call GitHub still
            # construct cleanly.
            self._token = None
        self.cache = _ETagStore(cache_dir or _default_cache_dir())
        self.log = _AccessLog(log_path or _default_log_path())
        self._lock = threading.Lock()
        self._rate: dict[str, RateSnapshot] = {
            "core": RateSnapshot(bucket="core"),
            "graphql": RateSnapshot(bucket="graphql"),
        }
        self._last_call_at: float = 0.0
        self._rate_cap_hz = rate_cap_hz
        self._backpressure_threshold = backpressure_threshold
        # Per-reset-window dedupe for the low-water quota warning emitted
        # by `_record_rate`. Keyed by bucket → the reset_at of the window
        # for which a warning has already been printed. Survives until the
        # reset_at passes, at which point `low_water_buckets()` stops
        # reporting the bucket as low.
        self._quota_warn_state: dict[str, float] = {}
        # Rolling buffer of recent calls, time-pruned on read, count-capped
        # on write. Used by the low-water warning to embed the top callers
        # in the last few minutes inline. Both `request()` and `gh_cli()`
        # append here; transport disambiguates them in summaries.
        self._recent_calls: collections.deque = collections.deque(maxlen=5000)

        self._client = httpx.Client(
            base_url=f"https://api.{host}",
            timeout=DEFAULT_TIMEOUT,
            transport=transport,
            headers={
                "Accept": DEFAULT_ACCEPT,
                "User-Agent": user_agent or _default_user_agent(),
                "X-GitHub-Api-Version": DEFAULT_API_VERSION,
            },
        )
        if self._token:
            self._client.headers["Authorization"] = f"Bearer {self._token}"
        if trim_cache_on_init:
            try:
                self.cache.trim()
            except Exception:
                pass

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass
        self.log.close()

    # --- internals ---

    def _set_bearer_locked(self, token: str | None) -> None:
        self._token = token
        if token:
            self._client.headers["Authorization"] = f"Bearer {token}"
        else:
            self._client.headers.pop("Authorization", None)

    def _record_rate(self, fallback_bucket: str,
                     headers: httpx.Headers) -> RateSnapshot:
        try:
            limit = int(headers.get("x-ratelimit-limit", 0))
            remaining = int(headers.get("x-ratelimit-remaining", 0))
            reset = int(headers.get("x-ratelimit-reset", 0))
        except (ValueError, TypeError):
            with self._lock:
                return self._rate.get(fallback_bucket,
                                       RateSnapshot(bucket=fallback_bucket))
        resource = (headers.get("x-ratelimit-resource") or
                    fallback_bucket).lower()
        if resource not in ("core", "graphql"):
            resource = fallback_bucket
        snap = RateSnapshot(bucket=resource, limit=limit,
                            remaining=remaining,
                            reset_at=float(reset),
                            observed_at=time.time())
        with self._lock:
            self._rate[resource] = snap
        self._maybe_warn_quota_low(snap)
        return snap

    # When a bucket falls below this many remaining calls and the reset is
    # still far enough away that backpressure can't paper over it, emit a
    # one-shot stderr warning so the user knows quota is being burned and
    # can investigate via `pod gh-stats`. Tuned slightly above the
    # backpressure threshold so the warning fires before the layer starts
    # sleeping.
    _QUOTA_LOW_THRESHOLD = 100
    # Skip the warning when the bucket is near reset anyway — a bursty
    # caller hitting 99 remaining with 30s on the clock doesn't need a
    # stderr nag every reset window.
    _QUOTA_WARN_MIN_SECS_TO_RESET = 120
    # Window over which we summarise recent callers in the warning. Tied
    # to `_recent_calls` capacity (5000 entries) — at the per-process
    # rate cap of 50/s, this is well over 1 minute of full saturation,
    # which is plenty for "what caused the drop" diagnosis.
    _QUOTA_WARN_CALLER_WINDOW_S = 300.0

    def _remember_call(self, ts: float, caller: str, bucket: str,
                       transport: str) -> None:
        """Record an attempted call in the rolling buffer used by the
        low-water warning. Called from both `request()` (before the
        network round-trip, so attempts that error out still register)
        and `gh_cli()`. Safe under `self._lock`."""
        with self._lock:
            self._recent_calls.append((ts, caller, bucket, transport))

    def _maybe_warn_quota_low(self, snap: RateSnapshot) -> None:
        if not snap.limit or snap.remaining >= self._QUOTA_LOW_THRESHOLD:
            return
        secs_to_reset = max(0, int(snap.reset_at - time.time()))
        if secs_to_reset < self._QUOTA_WARN_MIN_SECS_TO_RESET:
            # Bucket will refill before any reasonable mitigation could
            # take effect; don't bother the user.
            return
        cutoff = time.time() - self._QUOTA_WARN_CALLER_WINDOW_S
        with self._lock:
            last = self._quota_warn_state.get(snap.bucket, 0.0)
            if last == snap.reset_at:
                return
            self._quota_warn_state[snap.bucket] = snap.reset_at
            recent = [(c, t) for (ts, c, b, t) in self._recent_calls
                      if ts >= cutoff and b == snap.bucket]
        counts: collections.Counter = collections.Counter(
            (c, t) for (c, t) in recent)
        top = counts.most_common(3)
        reset_iso = _iso_at(snap.reset_at) or "unknown"
        msg = (f"pod: gh-quota: {snap.bucket} bucket low: "
               f"{snap.remaining}/{snap.limit} remaining, "
               f"resets at {reset_iso} (~{secs_to_reset}s)")
        if top:
            parts = []
            for (caller, transport), n in top:
                suffix = " (gh-cli)" if transport == "gh" else ""
                parts.append(f"{caller}{suffix} ×{n}")
            msg += "; recent callers: " + ", ".join(parts)
        msg += "; run `pod gh-stats` for full breakdown.\n"
        sys.stderr.write(msg)

    def low_water_buckets(self) -> set[str]:
        """Names of buckets that have crossed the low-water threshold and
        whose reset window has not yet passed. Used by the TUI header to
        show a `graphql:!` / `core:!` indicator while quota is depleted —
        keeps the warning visible even when curses overwrites stderr."""
        now = time.time()
        with self._lock:
            return {b for b, ra in self._quota_warn_state.items()
                    if ra > now}

    def _backpressure(self, bucket: str) -> None:
        """Sleep before the next call if the bucket is near exhaustion or
        the per-process rate cap would be exceeded."""
        with self._lock:
            snap = self._rate.get(bucket, RateSnapshot(bucket=bucket))
            cap_hz = self._rate_cap_hz
            last_call = self._last_call_at
        if (snap.limit and
                snap.remaining < self._backpressure_threshold and
                snap.reset_at):
            wait = min(MAX_BACKPRESSURE_SLEEP,
                       max(0.0, snap.reset_at - time.time()))
            if wait > 0:
                time.sleep(wait)
        if cap_hz > 0:
            min_gap = 1.0 / cap_hz
            now = time.time()
            gap = now - last_call
            if gap < min_gap:
                time.sleep(min_gap - gap)
        with self._lock:
            self._last_call_at = time.time()

    def _caller(self) -> str:
        """Walk the stack to find the first frame that's neither inside
        pod/github.py nor a known thin wrapper (`_gh_cli` in cli.py).
        That's the user of the client, regardless of how many wrapper
        layers sit between them and `request()`."""
        this_file = __file__
        # Functions we treat as transparent wrappers — skip them too so
        # the caller column points at the actual semantic caller.
        skip_funcs = {"_gh_cli"}
        st = inspect.stack()
        for f in st[1:]:
            if f.filename == this_file:
                continue
            if f.function in skip_funcs:
                continue
            return f"{Path(f.filename).name}:{f.function}:{f.lineno}"
        return "unknown"

    def _refresh_token(self) -> bool:
        """Re-fetch via `gh auth token` and swap the bearer; True iff changed."""
        try:
            new = _gh_token(self.host)
        except Exception:
            return False
        if not new:
            return False
        with self._lock:
            if new == self._token:
                return False
            self._set_bearer_locked(new)
        return True

    # --- request ---

    def request(self, method: str, path_or_url: str, *,
                params: dict | None = None,
                json: Any = None,
                headers: dict[str, str] | None = None,
                accept: str = DEFAULT_ACCEPT,
                api_version: str = DEFAULT_API_VERSION,
                cache: Literal["etag", "none"] = "etag",
                timeout: float = DEFAULT_TIMEOUT,
                _allow_retry: bool = True,
                _caller: str | None = None) -> GHResponse:
        method = method.upper()
        # ETag conditional requests are only useful on GET/HEAD. We
        # used to also send `If-None-Match` on POST `/graphql` on the
        # belief that GitHub returns 304 for unchanged queries, but
        # field data (zero 304s out of ~14k GraphQL POSTs over four
        # hours) shows the server ignores the header and bills every
        # POST against the GraphQL bucket. The body-keyed value cache
        # in cli.py (`_ProvenanceDiskCache`) is what actually keeps
        # provenance fetches off the wire.
        cacheable = cache == "etag" and method in ("GET", "HEAD")

        if path_or_url.startswith("http"):
            url = path_or_url
            url_for_key = path_or_url
        else:
            if not path_or_url.startswith("/"):
                path_or_url = "/" + path_or_url
            url = path_or_url
            url_for_key = path_or_url

        # Build a logging-friendly URL that includes any params, then run
        # it through redaction (so secrets in query strings don't leak).
        if params:
            url_for_log = url_for_key
            sep = "&" if "?" in url_for_log else "?"
            url_for_log = url_for_log + sep + urllib.parse.urlencode(
                sorted((k, v) for k, v in params.items()), doseq=True)
        else:
            url_for_log = url_for_key

        send_headers: dict[str, str] = {}
        if accept != DEFAULT_ACCEPT:
            send_headers["Accept"] = accept
        if api_version != DEFAULT_API_VERSION:
            send_headers["X-GitHub-Api-Version"] = api_version
        if headers:
            send_headers.update(headers)

        cache_key: str | None = None
        cached: dict | None = None
        if cacheable:
            cache_key = _ETagStore.key_for(self.host, method, url_for_key,
                                            params, accept, api_version,
                                            body=json)
            cached = self.cache.load(cache_key)
            if cached and cached.get("etag"):
                send_headers["If-None-Match"] = cached["etag"]

        bucket = _bucket_for_path(url_for_key)
        self._backpressure(bucket)

        caller = _caller or self._caller()
        t0 = time.time()
        # Record the attempt now so a network exception that aborts the
        # request still shows up in the recent-callers buffer used by
        # `_maybe_warn_quota_low`.
        self._remember_call(t0, caller, bucket, "httpx")
        try:
            resp = self._client.request(
                method, url, params=params, json=json,
                headers=send_headers, timeout=timeout)
        except httpx.HTTPError as e:
            self.log.write(
                ts=_iso_now(), verb=method,
                url=_redact_url(url_for_log), status=0,
                ms=int((time.time() - t0) * 1000),
                cache_hit=False, caller=caller, bucket=bucket,
                error=str(e)[:200], transport="httpx")
            raise
        elapsed_ms = (time.time() - t0) * 1000

        # 401 → refresh bearer once, retry once.
        if resp.status_code == 401 and _allow_retry:
            if self._refresh_token():
                return self.request(
                    method, path_or_url, params=params, json=json,
                    headers=headers, accept=accept,
                    api_version=api_version, cache=cache, timeout=timeout,
                    _allow_retry=False, _caller=caller)

        # Retry-After: sleep then retry once.
        retry_after = resp.headers.get("retry-after")
        if (resp.status_code in (403, 429) and retry_after and _allow_retry):
            try:
                wait = min(MAX_BACKPRESSURE_SLEEP, float(retry_after))
            except ValueError:
                wait = 0.0
            if wait > 0:
                time.sleep(wait)
                return self.request(
                    method, path_or_url, params=params, json=json,
                    headers=headers, accept=accept,
                    api_version=api_version, cache=cache, timeout=timeout,
                    _allow_retry=False, _caller=caller)

        rate = self._record_rate(bucket, resp.headers)

        cache_hit = False
        body: Any = None
        body_cached: Any = None
        if cacheable and resp.status_code == 304 and cached:
            cache_hit = True
            body_cached = cached.get("body")
        elif cacheable and resp.status_code == 200:
            etag = resp.headers.get("etag")
            try:
                body = resp.json()
            except Exception:
                body = None
            if etag and body is not None and cache_key is not None:
                self.cache.save(cache_key, etag=etag, body=body,
                                status=200, url=url_for_key,
                                headers=dict(resp.headers))
        else:
            try:
                body = resp.json()
            except Exception:
                body = None

        next_url = _parse_next_link(resp.headers.get("link", ""))

        self.log.write(
            ts=_iso_now(), verb=method,
            url=_redact_url(url_for_log),
            status=resp.status_code, ms=int(elapsed_ms),
            cache_hit=cache_hit, caller=caller, bucket=bucket,
            remaining=rate.remaining if rate.limit else None,
            reset=_iso_at(rate.reset_at),
            bytes=len(resp.content) if resp.content else 0,
            transport="httpx",
        )

        return GHResponse(
            status=resp.status_code,
            json=body,
            body_cached=body_cached,
            headers={k: v for k, v in resp.headers.items()},
            ms=elapsed_ms,
            cache_hit=cache_hit,
            rate=rate,
            raw=resp.content,
            next_url=next_url,
        )

    # --- HTTP verb shortcuts ---

    def get(self, path: str, *, params: dict | None = None,
            **kw: Any) -> GHResponse:
        return self.request("GET", path, params=params, **kw)

    def post(self, path: str, *, json: Any = None, **kw: Any) -> GHResponse:
        kw.setdefault("cache", "none")
        return self.request("POST", path, json=json, **kw)

    def put(self, path: str, *, json: Any = None, **kw: Any) -> GHResponse:
        kw.setdefault("cache", "none")
        return self.request("PUT", path, json=json, **kw)

    def patch(self, path: str, *, json: Any = None, **kw: Any) -> GHResponse:
        kw.setdefault("cache", "none")
        return self.request("PATCH", path, json=json, **kw)

    def delete(self, path: str, **kw: Any) -> GHResponse:
        kw.setdefault("cache", "none")
        return self.request("DELETE", path, **kw)

    # --- pagination ---

    def paginate(self, path: str, *, params: dict | None = None,
                 per_page: int = DEFAULT_PER_PAGE,
                 max_pages: int | None = None,
                 cache: Literal["etag", "none"] = "etag"
                 ) -> Iterator[GHResponse]:
        """Walk Link-header pagination. Each page is independently ETag'd
        with per_page in the cache key; a 304 on page N does not imply 304
        on any other page."""
        params = dict(params or {})
        params["per_page"] = per_page
        next_url: str | None = path
        page = 0
        while next_url is not None:
            page += 1
            if max_pages is not None and page > max_pages:
                return
            send_params = params if page == 1 else None
            resp = self.request("GET", next_url, params=send_params,
                                cache=cache)
            yield resp
            if not resp.ok():
                return
            next_url = resp.next_url

    # --- GraphQL ---

    def graphql(self, query: str, variables: dict | None = None,
                *, cache: Literal["etag", "none"] = "none") -> GHResponse:
        body: dict[str, Any] = {"query": query}
        if variables:
            body["variables"] = variables
        return self.request("POST", "/graphql", json=body, cache=cache)

    # --- gh CLI passthrough ---

    def gh_cli(self, *argv: str, timeout: float = 60,
               input: bytes | str | None = None,
               text: bool = True,
               check: bool = False) -> subprocess.CompletedProcess:
        """Run a `gh` subprocess. Logged + back-pressured; returns a
        CompletedProcess so call sites can treat it identically to a
        direct subprocess.run. Defaults to `text=True`, matching how
        every existing pod call site reads `r.stdout`."""
        self._backpressure("core")
        caller = self._caller()
        # If input is binary, force text=False so we don't fail on encoding.
        if isinstance(input, bytes):
            text = False
        t0 = time.time()
        # `gh_cli` burns the core bucket just like a direct HTTP call.
        # Record it so PR-heavy workflows that lean on `gh pr ...` show up
        # in the recent-callers summary instead of silently misattributing
        # the bucket drain to the few HTTP calls in the window.
        self._remember_call(t0, caller, "core", "gh")
        try:
            r = subprocess.run(
                ["gh", *argv],
                capture_output=True,
                input=input,
                text=text,
                timeout=timeout,
                check=check,
            )
            returncode = r.returncode
        except subprocess.TimeoutExpired:
            self.log.write(
                ts=_iso_now(), verb="GH",
                url=" ".join(_redact_argv(argv)),
                status=-1, ms=int((time.time() - t0) * 1000),
                cache_hit=False, caller=caller, bucket="core",
                transport="gh", error="timeout")
            raise
        elapsed_ms = (time.time() - t0) * 1000
        self.log.write(
            ts=_iso_now(), verb="GH",
            url=" ".join(_redact_argv(argv)),
            status=returncode, ms=int(elapsed_ms),
            cache_hit=False, caller=caller, bucket="core",
            transport="gh",
        )
        return r

    # --- domain helpers ---

    def list_open_issues(self, repo: str, *,
                         labels: list[str] | None = None,
                         exclude_prs: bool = True,
                         per_page: int = 100,
                         max_pages: int | None = None) -> list[dict]:
        """List open issues in `repo` (\"owner/name\").

        GitHub's REST `/repos/{slug}/issues` endpoint includes pull
        requests in the response (objects carrying a `pull_request`
        field), unlike `gh issue list` which excludes them. We default
        `exclude_prs=True` so callers get the `gh issue list` shape
        unless they explicitly opt out. Routes through the layer so
        every page is ETag-cached.
        """
        params: dict[str, str] = {"state": "open"}
        if labels:
            params["labels"] = ",".join(labels)
        out: list[dict] = []
        for page in self.paginate(f"/repos/{repo}/issues",
                                  params=params,
                                  per_page=per_page,
                                  max_pages=max_pages):
            if not page.ok():
                break
            body = page.body() or []
            for item in body:
                if exclude_prs and item.get("pull_request"):
                    continue
                out.append(item)
        return out

    # --- introspection ---

    def rate(self) -> dict[str, RateSnapshot]:
        with self._lock:
            return dict(self._rate)


# ---------------------------------------------------------------------------
# Module-global client
# ---------------------------------------------------------------------------

_client_lock = threading.Lock()
_client: GitHubClient | None = None


def get_client() -> GitHubClient:
    """Return the per-process client, constructing it on first access."""
    global _client
    with _client_lock:
        if _client is None:
            _client = GitHubClient()
        return _client


def set_client_for_tests(client: GitHubClient | None) -> None:
    """Test-only: replace (or clear) the cached module-global client."""
    global _client
    with _client_lock:
        if _client is not None and _client is not client:
            try:
                _client.close()
            except Exception:
                pass
        _client = client
