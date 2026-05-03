"""Tests for the worktree self-healing path and disk-space guard.

These cover the changes that prevent the dispatch -> error -> dispatch
loop seen after a disk-full event:

  - ``_scrub_worktree_remnants`` is idempotent and clears all four kinds
    of leftover state (branch ref, branch lock, admin dir, worktree dir).
  - ``setup_worktree`` re-raises with stderr embedded on failure, and
    scrubs partial state so retries get a clean slate.
  - ``cleanup_stale_branches`` deletes orphan ``agent/*`` refs but
    respects live agents, existing worktrees, and the min-age window.
  - ``_disk_low`` correctly classifies free-disk values against the
    configured threshold.
"""
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


def _git(args, cwd):
    """Run a git command, raising if it fails."""
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True, check=True, text=True,
    )


def _init_repo(root: Path) -> Path:
    """Create a bare-bones repo with one commit so worktrees can branch off.

    Adds a self-pointing ``origin`` remote and fetches it so that
    ``setup_worktree`` (which always uses ``origin/<base>``) works.
    """
    _git(["init", "-q", "-b", "main"], cwd=root)
    _git(["config", "user.email", "test@example.com"], cwd=root)
    _git(["config", "user.name", "test"], cwd=root)
    (root / "README").write_text("hi\n")
    _git(["add", "."], cwd=root)
    _git(["commit", "-q", "-m", "init"], cwd=root)
    _git(["remote", "add", "origin", str(root)], cwd=root)
    _git(["fetch", "-q", "origin", "main"], cwd=root)
    return root


class ScrubRemnantsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = _init_repo(Path(self._tmp.name))
        self._patches = [
            mock.patch.object(cli, "PROJECT_DIR", self.root),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def test_idempotent_when_nothing_exists(self):
        # Should not raise even though branch/dir/admin all absent
        cli._scrub_worktree_remnants("deadbeef", self.root / "worktrees" / "deadbeef")

    def test_removes_orphan_branch_ref(self):
        # Create a branch that points at HEAD but has no worktree
        _git(["branch", "agent/abc12345"], cwd=self.root)
        ref = self.root / ".git" / "refs" / "heads" / "agent" / "abc12345"
        self.assertTrue(ref.exists())

        cli._scrub_worktree_remnants("abc12345", self.root / "worktrees" / "abc12345")

        self.assertFalse(ref.exists(), "orphan branch ref should be deleted")

    def test_removes_stale_branch_lock(self):
        # Simulate a crashed git process that left a .lock file
        ref_dir = self.root / ".git" / "refs" / "heads" / "agent"
        ref_dir.mkdir(parents=True)
        lock = ref_dir / "abc12345.lock"
        lock.write_text("garbage\n")

        cli._scrub_worktree_remnants("abc12345", self.root / "worktrees" / "abc12345")

        self.assertFalse(lock.exists(), "stale branch lock should be cleaned up")

    def test_removes_existing_worktree_dir_and_admin_entry(self):
        # Make a real worktree, then verify scrub cleans both the dir and the
        # admin entry under .git/worktrees/.
        wt_dir = self.root / "worktrees" / "abc12345"
        _git(["worktree", "add", "-b", "agent/abc12345", str(wt_dir), "main", "--quiet"],
             cwd=self.root)
        admin = self.root / ".git" / "worktrees" / "abc12345"
        self.assertTrue(wt_dir.exists())
        self.assertTrue(admin.exists())

        cli._scrub_worktree_remnants("abc12345", wt_dir)

        self.assertFalse(wt_dir.exists())
        self.assertFalse(admin.exists(), "git worktree prune should drop the admin entry")
        # And the branch
        ref = self.root / ".git" / "refs" / "heads" / "agent" / "abc12345"
        self.assertFalse(ref.exists())


class SetupWorktreeFailureTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = _init_repo(Path(self._tmp.name))
        self._patches = [
            mock.patch.object(cli, "PROJECT_DIR", self.root),
            mock.patch.object(cli, "_get_base_branch", return_value="main"),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def test_success_creates_worktree_and_branch(self):
        wt_dir, branch = cli.setup_worktree({}, "11111111")
        self.assertTrue(Path(wt_dir).exists())
        self.assertEqual(branch, "agent/11111111")
        # Cleanup so worktree handles don't leak between tests
        cli._scrub_worktree_remnants("11111111", Path(wt_dir))

    def test_failure_scrubs_partial_state_and_surfaces_stderr(self):
        # Pre-create the destination as a *file* so `git worktree add` fails
        # with a clear error after creating the branch ref. This mimics the
        # disk-full / lock-contention failure mode where git creates the
        # branch then trips on the worktree directory.
        wt_dir = self.root / "worktrees" / "22222222"
        wt_dir.parent.mkdir(parents=True, exist_ok=True)
        wt_dir.write_text("not a directory\n")  # blocks worktree creation

        with self.assertRaises(subprocess.CalledProcessError) as cm:
            cli.setup_worktree({}, "22222222")

        # stderr from git is surfaced (decoded) in the re-raised exception
        stderr = (cm.exception.stderr or b"").decode(errors="replace")
        self.assertTrue(stderr, "stderr should be non-empty so the caller can log why git failed")
        self.assertIn("scrubbed partial state", stderr)

        # Partial state was scrubbed: no orphan branch ref left over
        ref = self.root / ".git" / "refs" / "heads" / "agent" / "22222222"
        self.assertFalse(ref.exists(),
                         "branch ref must be scrubbed so the next dispatch attempt can reuse the slot")

        # Clean up the blocker file
        wt_dir.unlink()


class CleanupStaleBranchesTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = _init_repo(Path(self._tmp.name))
        agents_dir = self.root / ".pod" / "agents"
        agents_dir.mkdir(parents=True)
        self._patches = [
            mock.patch.object(cli, "PROJECT_DIR", self.root),
            mock.patch.object(cli, "AGENTS_DIR", agents_dir),
            mock.patch.object(cli, "read_all_agents", return_value=[]),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def _make_branch(self, short_id: str, *, age_seconds: float = 1000.0) -> Path:
        _git(["branch", f"agent/{short_id}"], cwd=self.root)
        ref = self.root / ".git" / "refs" / "heads" / "agent" / short_id
        if age_seconds:
            past = ref.stat().st_mtime - age_seconds
            os.utime(ref, (past, past))
        return ref

    def test_deletes_orphan_branch_with_no_worktree(self):
        ref = self._make_branch("aaaaaaaa")

        deleted = cli.cleanup_stale_branches({}, min_age=0)

        self.assertEqual(deleted, 1)
        self.assertFalse(ref.exists())

    def test_skips_branch_with_existing_worktree_dir(self):
        ref = self._make_branch("bbbbbbbb")
        # Worktree dir exists (e.g. an in-flight setup_worktree just created it)
        (self.root / "worktrees" / "bbbbbbbb").mkdir(parents=True)

        deleted = cli.cleanup_stale_branches({}, min_age=0)

        self.assertEqual(deleted, 0)
        self.assertTrue(ref.exists())

    def test_skips_branch_referenced_by_live_agent(self):
        ref = self._make_branch("cccccccc")

        live = cli.AgentState(
            uuid="some-session", short_id="agent01",
            branch="agent/cccccccc", status="working",
        )
        with mock.patch.object(cli, "read_all_agents", return_value=[live]):
            deleted = cli.cleanup_stale_branches({}, min_age=0)

        self.assertEqual(deleted, 0)
        self.assertTrue(ref.exists())

    def test_skips_young_branch(self):
        # Just-created (mtime = now)
        ref = self._make_branch("dddddddd", age_seconds=0)

        deleted = cli.cleanup_stale_branches({}, min_age=300)

        self.assertEqual(deleted, 0,
                         "must not delete a branch that may belong to an in-flight setup_worktree")
        self.assertTrue(ref.exists())


class DiskLowTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        patcher = mock.patch.object(cli, "PROJECT_DIR", self.root)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _patch_free(self, free_gib: float):
        # disk_usage returns a namedtuple-like with .free in bytes
        usage = mock.MagicMock()
        usage.free = int(free_gib * (1024 ** 3))
        return mock.patch("pod.cli.shutil.disk_usage", return_value=usage)

    def test_returns_none_when_disk_above_threshold(self):
        with self._patch_free(50.0):
            self.assertIsNone(cli._disk_low({"project": {"min_free_disk_gb": 2}}))

    def test_returns_reason_when_below_threshold(self):
        with self._patch_free(0.5):
            reason = cli._disk_low({"project": {"min_free_disk_gb": 2}})
            self.assertIsNotNone(reason)
            self.assertIn("0.50", reason)
            self.assertIn("2.00", reason)

    def test_threshold_zero_disables_check(self):
        with self._patch_free(0.001):
            self.assertIsNone(cli._disk_low({"project": {"min_free_disk_gb": 0}}))

    def test_default_threshold_is_two_gb(self):
        with self._patch_free(1.5):
            self.assertIsNotNone(cli._disk_low({}))
        with self._patch_free(5.0):
            self.assertIsNone(cli._disk_low({}))


if __name__ == "__main__":
    unittest.main()
