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
        cli._security_checked = False

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

    def test_idempotent_within_process(self):
        fake = _mock_run("private", "owner/repo", "{}")
        with mock.patch.object(cli.subprocess, "run", side_effect=fake) as run:
            cli.check_repo_security({})
            cli.check_repo_security({})
        # Only the first call should have hit subprocess.
        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
