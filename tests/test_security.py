import datetime
import unittest
from unittest import mock

from pod import cli


def _mock_run(visibility: str, slug: str, limit_response: str | None,
              limit_returncode: int = 0):
    """Return a fake subprocess.run that responds to the two gh calls
    check_repo_security makes (gh repo view, then gh api interaction-limits).
    """
    repo_view = mock.Mock()
    repo_view.returncode = 0
    repo_view.stdout = (
        '{"visibility":"' + visibility + '","nameWithOwner":"' + slug + '"}\n'
    )
    repo_view.stderr = ""

    api = mock.Mock()
    api.returncode = limit_returncode
    api.stdout = limit_response if limit_response is not None else ""
    api.stderr = "" if limit_returncode == 0 else "API error"

    def fake_run(argv, **kwargs):
        if argv[:2] == ["gh", "repo"]:
            return repo_view
        if argv[:2] == ["gh", "api"]:
            return api
        raise AssertionError(f"unexpected subprocess call: {argv}")

    return fake_run


def _iso(days_from_now: float) -> str:
    t = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days_from_now)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class SecurityCheckTests(unittest.TestCase):
    def setUp(self):
        cli._security_last_ok = 0.0

    def test_disabled_skips_all_checks(self):
        with mock.patch.object(cli.subprocess, "run") as run:
            cli.check_repo_security({"security": {"enforce_interaction_limits": False}})
        run.assert_not_called()

    def test_private_repo_passes(self):
        fake = _mock_run("private", "owner/repo", "{}")
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            cli.check_repo_security({})  # no exit

    def test_public_no_limit_refused(self):
        fake = _mock_run("public", "owner/repo", "{}")
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            with self.assertRaises(SystemExit) as cm:
                cli.check_repo_security({})
        self.assertEqual(cm.exception.code, 1)

    def test_public_collaborators_only_long_expiry_passes(self):
        body = (
            '{"limit":"collaborators_only","origin":"repository",'
            '"expires_at":"' + _iso(180) + '"}'
        )
        fake = _mock_run("public", "owner/repo", body)
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            cli.check_repo_security({})

    def test_public_limit_too_lax_refused(self):
        body = (
            '{"limit":"existing_users","origin":"repository",'
            '"expires_at":"' + _iso(180) + '"}'
        )
        fake = _mock_run("public", "owner/repo", body)
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_public_expiring_soon_refused(self):
        body = (
            '{"limit":"collaborators_only","origin":"repository",'
            '"expires_at":"' + _iso(3) + '"}'
        )
        fake = _mock_run("public", "owner/repo", body)
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_public_no_expiry_passes(self):
        body = '{"limit":"collaborators_only","origin":"repository"}'
        fake = _mock_run("public", "owner/repo", body)
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            cli.check_repo_security({})

    def test_lower_minimum_accepts_weaker_limit(self):
        body = (
            '{"limit":"contributors_only","origin":"repository",'
            '"expires_at":"' + _iso(180) + '"}'
        )
        fake = _mock_run("public", "owner/repo", body)
        with mock.patch.object(cli.subprocess, "run", side_effect=fake):
            cli.check_repo_security(
                {"security": {"minimum_interaction_limit": "contributors_only"}}
            )

    def test_ttl_skips_recheck(self):
        fake = _mock_run("private", "owner/repo", "{}")
        with mock.patch.object(cli.subprocess, "run", side_effect=fake) as run:
            cli.check_repo_security({})
            cli.check_repo_security({})
        # Within TTL — only the first call should have hit subprocess.
        self.assertEqual(run.call_count, 1)

    def test_ttl_expires(self):
        fake = _mock_run("private", "owner/repo", "{}")
        with mock.patch.object(cli.subprocess, "run", side_effect=fake) as run:
            cli.check_repo_security({})
            # Simulate TTL elapsing.
            cli._security_last_ok = (
                cli._security_last_ok - cli._SECURITY_CHECK_TTL_SECONDS - 1
            )
            cli.check_repo_security({})
        self.assertEqual(run.call_count, 2)

    def test_gh_not_found_fails_closed(self):
        with mock.patch.object(cli.subprocess, "run", side_effect=FileNotFoundError):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_gh_repo_view_error_fails_closed(self):
        bad = mock.Mock()
        bad.returncode = 1
        bad.stdout = ""
        bad.stderr = "auth required"
        with mock.patch.object(cli.subprocess, "run", return_value=bad):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})

    def test_gh_api_network_error_fails_closed(self):
        repo_view = mock.Mock(returncode=0, stderr="",
                              stdout='{"visibility":"public","nameWithOwner":"o/r"}')
        api_err = mock.Mock(returncode=1, stdout="", stderr="HTTP 500: server error")

        def fake_run(argv, **kwargs):
            return repo_view if argv[:2] == ["gh", "repo"] else api_err

        with mock.patch.object(cli.subprocess, "run", side_effect=fake_run):
            with self.assertRaises(SystemExit):
                cli.check_repo_security({})


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


if __name__ == "__main__":
    unittest.main()
