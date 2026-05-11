import contextlib
import datetime
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from pod import cli

from _gh_helpers import fake_response, patch_client


def _git_remote_mock(slug: str):
    return mock.Mock(returncode=0, stderr="",
                     stdout=f"git@github.com:{slug}.git\n")


@contextlib.contextmanager
def _mock_security(visibility: str, slug: str,
                    limit_response: dict | str | None = None,
                    limit_status: int = 200):
    """Combine `git remote` (subprocess.run) + the GitHub access layer
    (httpx via _FakeClient) for `check_repo_security`.

    After the B3 burner rewrite the visibility + interaction-limits
    check is a single GraphQL query, so we route a single POST `/graphql`
    response with the combined GraphQL shape:
      data.repository.visibility (PUBLIC/PRIVATE/INTERNAL)
      data.repository.interactionAbility { limit expiresAt origin }

    `limit_response` accepts the REST shape (dict with `limit`,
    `expires_at`, `origin`) for backwards compatibility with existing
    callers; we translate to the GraphQL shape internally.
    """
    if isinstance(limit_response, str):
        try:
            limit_body = json.loads(limit_response)
        except (json.JSONDecodeError, TypeError):
            limit_body = {}
    else:
        limit_body = limit_response or {}

    if limit_status != 200:
        gql_body = {"errors": [{"message": "interaction-limits failed"}]}
        graphql_status = limit_status
    else:
        # The GraphQL `InteractionAbility.limit` enum uses upper-case
        # (COLLABORATORS_ONLY); the production code lower-cases it before
        # comparing against config. Convert the REST-shaped fixture for
        # tests that still pass `"collaborators_only"` etc.
        raw_limit = limit_body.get("limit")
        gql_limit = raw_limit.upper() if isinstance(raw_limit, str) and raw_limit else None
        ability = None
        if limit_body:
            ability = {
                "limit": gql_limit,
                "expiresAt": limit_body.get("expires_at"),
                "origin": limit_body.get("origin"),
            }
        gql_body = {
            "data": {
                "repository": {
                    "visibility": visibility.upper() if visibility else None,
                    "interactionAbility": ability,
                }
            }
        }
        graphql_status = 200

    routes = {
        ("POST", "/graphql"): fake_response(graphql_status, body=gql_body),
    }

    git_remote = _git_remote_mock(slug)

    def fake_run(argv, **kwargs):
        if argv[:2] == ["git", "remote"]:
            return git_remote
        raise AssertionError(f"unexpected subprocess call: {argv}")

    with patch_client(routes=routes):
        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            yield


# Backwards-compat shim: existing tests still call `_mock_run(...)` and
# wrap it in `mock.patch.object(cli.subprocess, "run", side_effect=...)`.
# Returning a tuple of (context_manager, dummy_side_effect) would force
# rewriting every test; instead, expose `_mock_security` as the new
# context manager and migrate call sites individually.
def _mock_run(visibility: str, slug: str, limit_response: str | None,
              limit_returncode: int = 0):
    """Deprecated shim. Use `_mock_security(...)` directly with `with`."""
    raise RuntimeError(
        "_mock_run() shim removed; use `with _mock_security(...):` instead. "
        "See test_security.py for examples.")


def _iso(days_from_now: float) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days_from_now)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class SecurityCheckTests(unittest.TestCase):
    def setUp(self):
        cli._security_last_ok = 0.0
        cli._cached_repo_name = None
        # Redirect the disk cache to a private tempdir so tests don't
        # pollute (or get polluted by) the source repo's .pod directory.
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._cache_path = Path(self._tmp.name) / "security-cache.json"
        p = mock.patch.object(cli, "_security_cache_path",
                              return_value=self._cache_path)
        p.start()
        self.addCleanup(p.stop)

    def test_disabled_skips_all_checks(self):
        with mock.patch.object(cli.subprocess, "run") as run:
            cli.check_repo_security({"security": {"enforce_interaction_limits": False}})
        run.assert_not_called()

    def test_private_repo_passes(self):
        with _mock_security("private", "owner/repo"):
            cli.check_repo_security({})  # no exit

    def test_public_no_limit_refused(self):
        with _mock_security("public", "owner/repo", limit_response={}):
            with self.assertRaises(SystemExit) as cm:
                cli.check_repo_security({})
        self.assertEqual(cm.exception.code, 1)

    def test_public_collaborators_only_long_expiry_passes(self):
        body = {"limit": "collaborators_only",
                "origin": "repository",
                "expires_at": _iso(180)}
        with _mock_security("public", "owner/repo", limit_response=body):
            cli.check_repo_security({})

    def test_public_limit_too_lax_refused(self):
        body = {"limit": "existing_users",
                "origin": "repository",
                "expires_at": _iso(180)}
        with _mock_security("public", "owner/repo", limit_response=body):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_public_expiring_soon_refused(self):
        body = {"limit": "collaborators_only",
                "origin": "repository",
                "expires_at": _iso(3)}
        with _mock_security("public", "owner/repo", limit_response=body):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_public_no_expiry_passes(self):
        body = {"limit": "collaborators_only", "origin": "repository"}
        with _mock_security("public", "owner/repo", limit_response=body):
            cli.check_repo_security({})

    def test_lower_minimum_accepts_weaker_limit(self):
        body = {"limit": "contributors_only",
                "origin": "repository",
                "expires_at": _iso(180)}
        with _mock_security("public", "owner/repo", limit_response=body):
            cli.check_repo_security(
                {"security": {"minimum_interaction_limit": "contributors_only"}}
            )

    def test_ttl_skips_recheck(self):
        with _mock_security("private", "owner/repo") as _:
            cli.check_repo_security({})
            # Within TTL — the second call must not hit the layer.
            with patch_client() as client:
                cli.check_repo_security({})
                self.assertEqual(client.calls, [])

    def test_ttl_expires(self):
        with _mock_security("private", "owner/repo"):
            cli.check_repo_security({})
            # Simulate both TTLs elapsing — in-memory and disk.
            cli._security_last_ok = (
                cli._security_last_ok - cli._SECURITY_CHECK_TTL_SECONDS - 1
            )
            self._cache_path.unlink(missing_ok=True)
            graphql_body = {"data": {"repository": {
                "visibility": "PRIVATE", "interactionAbility": None,
            }}}
            with patch_client(routes={
                ("POST", "/graphql"): fake_response(200, body=graphql_body),
            }) as client, mock.patch.object(cli.subprocess, "run",
                                              return_value=_git_remote_mock("owner/repo")):
                cli.check_repo_security({})
                self.assertTrue(any(c["method"] == "POST" for c in client.calls))

    def test_gh_not_found_fails_closed(self):
        # The layer fails to construct a client (no `gh auth token`); we
        # surface the error as SystemExit per the existing contract.
        with patch_client(routes={}, default=fake_response(
                500, body={"message": "gh missing"})), \
             mock.patch.object(cli.subprocess, "run",
                                side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_gh_repo_view_error_fails_closed(self):
        # Slug lookup (git remote) succeeds; the GraphQL visibility probe fails.
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(
                401, body={"message": "auth required"}),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("o/r")):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_gh_api_network_error_fails_closed(self):
        # GraphQL 5xx → REST fallback (no routes set → 404 default) →
        # fail-closed exit 1.
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(
                500, body={"message": "server error"}),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("o/r")):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_rest_fallback_when_graphql_rate_limited(self):
        # GraphQL returns 200 with a RATE_LIMITED error block and no
        # `data` — the empty-data path that previously failed-closed.
        # With the fallback, pod fetches `/repos/{slug}` and
        # `/repos/{slug}/interaction-limits` via REST and succeeds.
        rest_repo = {"visibility": "public", "private": False}
        rest_limit = {"limit": "collaborators_only",
                      "origin": "repository",
                      "expires_at": _iso(180)}
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body={
                "errors": [{"type": "RATE_LIMITED",
                            "message": "API rate limit exceeded"}],
                "data": None,
            }),
            ("GET", "/repos/owner/repo"): fake_response(200, body=rest_repo),
            ("GET", "/repos/owner/repo/interaction-limits"):
                fake_response(200, body=rest_limit),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("owner/repo")):
            cli.check_repo_security({})  # no exit

    def test_rest_fallback_when_graphql_5xx(self):
        # GraphQL returns 5xx → REST fallback path serves valid data.
        rest_repo = {"visibility": "public", "private": False}
        rest_limit = {"limit": "collaborators_only",
                      "origin": "repository",
                      "expires_at": _iso(180)}
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(
                502, body={"message": "bad gateway"}),
            ("GET", "/repos/owner/repo"): fake_response(200, body=rest_repo),
            ("GET", "/repos/owner/repo/interaction-limits"):
                fake_response(200, body=rest_limit),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("owner/repo")):
            cli.check_repo_security({})  # no exit

    def test_rest_fallback_visibility_via_private_flag(self):
        # Older REST shape: no `visibility` field, only `private` boolean.
        # Private repos skip the interaction-limits requirement.
        rest_repo = {"private": True}  # no `visibility` key
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body={
                "errors": [{"type": "RATE_LIMITED"}], "data": None,
            }),
            ("GET", "/repos/owner/repo"): fake_response(200, body=rest_repo),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("owner/repo")):
            cli.check_repo_security({})  # no exit

    def test_rest_fallback_no_limit_set(self):
        # Public repo, REST interaction-limits returns 204 (no limit set).
        rest_repo = {"private": False, "visibility": "public"}
        with patch_client(routes={
            ("POST", "/graphql"): fake_response(200, body={
                "errors": [{"type": "RATE_LIMITED"}], "data": None,
            }),
            ("GET", "/repos/owner/repo"): fake_response(200, body=rest_repo),
            ("GET", "/repos/owner/repo/interaction-limits"):
                fake_response(204, body=None),
        }), mock.patch.object(cli.subprocess, "run",
                              return_value=_git_remote_mock("owner/repo")):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_smart_routing_skips_graphql_when_bucket_dry(self):
        # When `client.rate()` reports graphql near-empty and core
        # healthy, the helper goes straight to REST and never POSTs to
        # /graphql. This avoids burning the bucket further on a call
        # that would fail anyway.
        from pod.github import RateSnapshot

        rest_repo = {"private": False, "visibility": "public"}
        rest_limit = {"limit": "collaborators_only",
                      "origin": "repository",
                      "expires_at": _iso(180)}
        routes = {
            ("GET", "/repos/owner/repo"): fake_response(200, body=rest_repo),
            ("GET", "/repos/owner/repo/interaction-limits"):
                fake_response(200, body=rest_limit),
        }
        with patch_client(routes=routes) as client, \
                mock.patch.object(cli.subprocess, "run",
                                   return_value=_git_remote_mock("owner/repo")):
            # Override the fake client's `rate()` for this test only.
            client.rate = lambda: {
                "core": RateSnapshot(bucket="core", limit=5000,
                                     remaining=4999),
                "graphql": RateSnapshot(bucket="graphql", limit=5000,
                                        remaining=10),  # near-dry
            }
            cli.check_repo_security({})
            # No POST /graphql call should have been issued.
            self.assertFalse(
                any(c["method"] == "POST" and c["path"] == "/graphql"
                    for c in client.calls),
                f"smart routing failed; calls={client.calls}",
            )


class SecurityCacheTests(unittest.TestCase):
    """The disk-persisted cache survives across pod invocations so the
    REST visibility / interaction-limits round-trip happens at most
    once per `_SECURITY_DISK_TTL_SECONDS` rather than once per pod
    startup. These tests exercise the cache load / save / invalidate
    paths directly."""

    def setUp(self):
        cli._security_last_ok = 0.0
        cli._cached_repo_name = None
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._cache_path = Path(self._tmp.name) / "security-cache.json"
        p = mock.patch.object(cli, "_security_cache_path",
                              return_value=self._cache_path)
        p.start()
        self.addCleanup(p.stop)

    # --- _load_security_cache ---

    def _seed(self, **fields):
        payload = {
            "slug": "owner/repo",
            "checked_at": time.time(),
            "visibility": "public",
            "interaction_limit": "collaborators_only",
            "expires_at": _iso(180),
            **fields,
        }
        self._cache_path.write_text(json.dumps(payload))

    def test_load_missing_file_returns_false(self):
        self.assertFalse(self._cache_path.exists())
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_unreadable_json_returns_false(self):
        self._cache_path.write_text("{not json")
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_slug_mismatch_returns_false(self):
        self._seed(slug="other/repo")
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_stale_age_returns_false(self):
        self._seed(checked_at=time.time() - cli._SECURITY_DISK_TTL_SECONDS - 1)
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_negative_age_returns_false(self):
        # Clock skew: cache is from the future.
        self._seed(checked_at=time.time() + 60)
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_private_visibility_returns_true(self):
        # Private repos skip the interaction-limits check entirely; the
        # cached visibility verdict is sufficient.
        self._seed(visibility="private", interaction_limit=None,
                   expires_at=None)
        self.assertTrue(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_public_collaborators_only_returns_true(self):
        self._seed()
        self.assertTrue(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_public_too_lax_for_minimum_returns_false(self):
        # Cache says contributors_only; current policy demands collaborators_only.
        self._seed(interaction_limit="contributors_only")
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_public_expiring_soon_returns_false(self):
        self._seed(expires_at=_iso(3))
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_public_no_expiry_returns_true(self):
        self._seed(expires_at=None)
        self.assertTrue(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    def test_load_unparseable_expiry_returns_false(self):
        self._seed(expires_at="not-a-date")
        self.assertFalse(
            cli._load_security_cache("owner/repo", "collaborators_only", 7))

    # --- _save_security_cache ---

    def test_save_writes_file_atomically(self):
        cli._save_security_cache("owner/repo", visibility="public",
                                 interaction_limit="collaborators_only",
                                 expires_at=_iso(180))
        self.assertTrue(self._cache_path.exists())
        data = json.loads(self._cache_path.read_text())
        self.assertEqual(data["slug"], "owner/repo")
        self.assertEqual(data["visibility"], "public")
        self.assertEqual(data["interaction_limit"], "collaborators_only")
        self.assertIn("checked_at", data)

    def test_save_creates_pod_dir_if_missing(self):
        # Force the cache path under a not-yet-created subdirectory.
        deep = Path(self._tmp.name) / "freshly-made" / "security-cache.json"
        with mock.patch.object(cli, "_security_cache_path", return_value=deep):
            cli._save_security_cache("owner/repo", visibility="private")
        self.assertTrue(deep.exists())

    def test_save_swallows_errors(self):
        # If the path is unwritable, _save_security_cache must not raise —
        # the cache is an optimisation, not a security boundary.
        bad = Path("/nonexistent/cannot-create/file.json")
        with mock.patch.object(cli, "_security_cache_path", return_value=bad):
            cli._save_security_cache("owner/repo", visibility="public",
                                     interaction_limit="collaborators_only",
                                     expires_at=_iso(180))
        # No exception means pass.

    # --- check_repo_security integration ---

    def test_check_uses_disk_cache_to_skip_rest(self):
        # Seed a fresh disk cache; the security check must skip the REST
        # round-trip entirely.
        self._seed()
        with mock.patch.object(cli, "_get_repo", return_value="owner/repo"), \
             mock.patch.object(cli.subprocess, "run") as run:
            cli.check_repo_security({})
        run.assert_not_called()

    def test_check_writes_disk_cache_on_success_private(self):
        with _mock_security("private", "owner/repo"):
            cli.check_repo_security({})
        self.assertTrue(self._cache_path.exists())
        data = json.loads(self._cache_path.read_text())
        self.assertEqual(data["visibility"], "private")

    def test_check_writes_disk_cache_on_success_public(self):
        body = {"limit": "collaborators_only",
                "origin": "repository",
                "expires_at": _iso(180)}
        with _mock_security("public", "owner/repo", limit_response=body):
            cli.check_repo_security({})
        self.assertTrue(self._cache_path.exists())
        data = json.loads(self._cache_path.read_text())
        self.assertEqual(data["visibility"], "public")
        self.assertEqual(data["interaction_limit"], "collaborators_only")

    def test_check_does_not_cache_on_failure(self):
        # Visibility=public + missing interaction-limits → SystemExit;
        # cache must not be written so the next run still tries fresh.
        with _mock_security("public", "owner/repo", limit_response={}):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})
        self.assertFalse(self._cache_path.exists())

    def test_disk_cache_survives_in_memory_ttl_reset(self):
        # The disk cache is the whole point: even if `_security_last_ok` is
        # cleared (new process), a fresh disk cache lets us skip REST.
        self._seed()
        cli._security_last_ok = 0.0  # simulate fresh process
        with mock.patch.object(cli, "_get_repo", return_value="owner/repo"), \
             mock.patch.object(cli.subprocess, "run") as run:
            cli.check_repo_security({})
        run.assert_not_called()


class ValidateSecurityConfigTests(unittest.TestCase):
    def test_defaults_pass(self):
        cli.validate_security_config({})

    def test_explicit_valid_minimum_passes(self):
        for v in ("existing_users", "contributors_only", "collaborators_only"):
            cli.validate_security_config({"security": {"minimum_interaction_limit": v}})

    def test_typo_minimum_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"minimum_interaction_limit": "colaborators_only"}}
            )

    def test_non_bool_enforce_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"enforce_interaction_limits": "yes"}}
            )

    def test_negative_expiry_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config({"security": {"minimum_expiry_days": -1}})

    def test_non_numeric_expiry_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config({"security": {"minimum_expiry_days": "seven"}})


class SpawnAgentSecurityGateTests(unittest.TestCase):
    """Verify spawn_agent invokes the security check before forking, so
    every dispatch path (cmd_add, TUI auto-spawn, dead-claim restart) is
    gated regardless of where the call originates."""

    def setUp(self):
        cli._security_last_ok = 0.0
        cli._cached_repo_name = None

    def test_spawn_agent_calls_check(self):
        # Patch os.fork so we never actually fork during the test.
        with mock.patch.object(cli, "check_repo_security") as chk, \
             mock.patch.object(cli.os, "fork", return_value=12345), \
             mock.patch.object(cli.os, "waitpid", return_value=(12345, 0)):
            cli.spawn_agent({})
        chk.assert_called_once_with({})

    def test_spawn_agent_aborts_when_check_fails(self):
        # check_repo_security exits → spawn_agent must propagate, never fork.
        def boom(_cfg):
            raise SystemExit(1)

        with mock.patch.object(cli, "check_repo_security", side_effect=boom), \
             mock.patch.object(cli.os, "fork") as fork:
            with self.assertRaises(SystemExit):
                cli.spawn_agent({})
        fork.assert_not_called()


def _prov(*, author_login="alice", author_assoc="OWNER", comments=()):
    return cli._IssueProvenance(
        repo="o/r", issue_num=1,
        author_login=author_login, author_association=author_assoc,
        comments=[
            cli._ProvenanceComment(comment_id=cid, login=login, association=assoc)
            for cid, login, assoc in comments
        ],
        fetched_at=datetime.datetime.now(datetime.timezone.utc).timestamp(),
    )


class IsTrustedTests(unittest.TestCase):
    def test_owner_no_comments_trusted(self):
        ok, reason = cli.is_trusted(_prov(), {})
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_member_collaborator_trusted(self):
        for assoc in ("OWNER", "MEMBER", "COLLABORATOR"):
            ok, _ = cli.is_trusted(_prov(author_assoc=assoc), {})
            self.assertTrue(ok, assoc)

    def test_none_author_rejected_with_login_in_reason(self):
        ok, reason = cli.is_trusted(
            _prov(author_login="attacker", author_assoc="NONE"), {})
        self.assertFalse(ok)
        self.assertIn("@attacker", reason)
        self.assertIn("NONE", reason)

    def test_contributor_rejected_by_default(self):
        # CONTRIBUTOR = "previously merged a PR" — not a trust signal.
        ok, reason = cli.is_trusted(
            _prov(author_login="bob", author_assoc="CONTRIBUTOR"), {})
        self.assertFalse(ok)
        self.assertIn("@bob", reason)

    def test_untrusted_comment_rejected_with_comment_id(self):
        ok, reason = cli.is_trusted(
            _prov(comments=[(12345, "attacker", "NONE")]), {})
        self.assertFalse(ok)
        self.assertIn("c#12345", reason)
        self.assertIn("@attacker", reason)

    def test_trusted_users_promotes_none_author(self):
        ok, _ = cli.is_trusted(
            _prov(author_login="dependabot[bot]", author_assoc="NONE"),
            {"security": {"trusted_users": ["dependabot[bot]"]}})
        self.assertTrue(ok)

    def test_trusted_users_promotes_none_commenter(self):
        ok, _ = cli.is_trusted(
            _prov(comments=[(1, "ci-bot", "NONE")]),
            {"security": {"trusted_users": ["ci-bot"]}})
        self.assertTrue(ok)

    def test_disabled_short_circuits(self):
        ok, _ = cli.is_trusted(
            _prov(author_assoc="NONE"),
            {"security": {"trust_only_collaborators": False}})
        self.assertTrue(ok)

    def test_custom_trusted_assocs(self):
        # Operator drops MEMBER from trust set — MEMBER author now rejected.
        ok, _ = cli.is_trusted(
            _prov(author_assoc="MEMBER"),
            {"security": {"trusted_author_associations":
                          ["OWNER", "COLLABORATOR"]}})
        self.assertFalse(ok)


class ProvenanceCacheTests(unittest.TestCase):
    def setUp(self):
        cli._provenance_cache.clear()

    def _patch_fetch(self, prov):
        # Patch the fetcher and visibility check; return mock for assertions.
        return mock.patch.multiple(
            cli,
            fetch_issue_provenance=mock.MagicMock(return_value=prov),
            _is_repo_public=mock.MagicMock(return_value=True),
        )

    def test_cached_call_reuses(self):
        prov = _prov()
        with self._patch_fetch(prov):
            cli.check_issue_provenance("o/r", 1, {})
            cli.check_issue_provenance("o/r", 1, {})
            self.assertEqual(cli.fetch_issue_provenance.call_count, 1)

    def test_fresh_bypasses_cache(self):
        prov = _prov()
        with self._patch_fetch(prov):
            cli.check_issue_provenance("o/r", 1, {})
            cli.check_issue_provenance("o/r", 1, {}, fresh=True)
            self.assertEqual(cli.fetch_issue_provenance.call_count, 2)

    def test_ttl_expiry_refetches(self):
        prov = _prov()
        with self._patch_fetch(prov):
            cli.check_issue_provenance("o/r", 1, {})
            # Age the cached entry past TTL.
            cached = cli._provenance_cache[("o/r", 1)]
            cached.fetched_at -= cli._provenance_ttl({}) + 1
            cli.check_issue_provenance("o/r", 1, {})
            self.assertEqual(cli.fetch_issue_provenance.call_count, 2)

    def test_private_repo_short_circuits(self):
        with mock.patch.object(cli, "_is_repo_public", return_value=False), \
             mock.patch.object(cli, "fetch_issue_provenance") as fetch:
            ok, _ = cli.check_issue_provenance("o/r", 1, {})
        self.assertTrue(ok)
        fetch.assert_not_called()

    def test_disabled_short_circuits(self):
        with mock.patch.object(cli, "fetch_issue_provenance") as fetch, \
             mock.patch.object(cli, "_is_repo_public", return_value=True):
            ok, _ = cli.check_issue_provenance(
                "o/r", 1, {"security": {"trust_only_collaborators": False}})
        self.assertTrue(ok)
        fetch.assert_not_called()


class ValidateProvenanceConfigTests(unittest.TestCase):
    def test_defaults_pass(self):
        cli.validate_security_config({})

    def test_explicit_associations_pass(self):
        cli.validate_security_config(
            {"security": {"trusted_author_associations":
                          ["OWNER", "COLLABORATOR"]}})

    def test_typo_association_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"trusted_author_associations": ["OWNERR"]}})

    def test_non_list_associations_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"trusted_author_associations": "OWNER"}})

    def test_non_list_trusted_users_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"trusted_users": "dependabot"}})

    def test_negative_cache_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"provenance_cache_seconds": -1}})

    def test_non_bool_trust_rejected(self):
        with self.assertRaises(SystemExit):
            cli.validate_security_config(
                {"security": {"trust_only_collaborators": "yes"}})


if __name__ == "__main__":
    unittest.main()
