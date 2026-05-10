"""Integration test: cmd_create_pr refuses to set has-pr when an
existing PR's body lacks a closing keyword for the issue.

The bug this guards against: a worker (or human) creates a PR
manually with body `Partial progress on #N`, then runs
`coordination create-pr N` (without --partial). Pre-fix, the
existing-PR branch unconditionally added `has-pr` to issue #N.
When the PR merged, GitHub did NOT auto-close the issue (no
`Closes #N`), so the issue stayed `has-pr` forever and was
silently excluded from the work queue.

The fix dies with a descriptive message before mutating any
labels, so the worker can either edit the PR body or rerun
with --partial.
"""
import os
import shutil
import stat
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
COORDINATION = PROJECT_ROOT / "pod" / "data" / "coordination"


def _make_stub_gh(stub_dir: Path, body: str, existing_pr_number: int = 42) -> Path:
    """Write a stub `gh` shell script that emulates just the API calls
    `cmd_create_pr` makes before the guard fires."""
    stub = stub_dir / "gh"
    stub.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        # Minimal gh stub for cmd_create_pr guard test. Routes by argv shape.
        set -e
        case "$1 $2" in
          "repo view")
            # `gh repo view --json nameWithOwner -q .nameWithOwner` etc.
            for arg in "$@"; do
              case "$arg" in
                .nameWithOwner) echo "owner/repo"; exit 0;;
                .defaultBranchRef.name) echo "main"; exit 0;;
              esac
            done
            exit 0;;
          "pr list")
            # First call: --head BRANCH --json number --jq '.[0].number // empty'
            echo "{existing_pr_number}"
            exit 0;;
          "pr view")
            # `gh pr view N --repo owner/repo --json body --jq .body`
            cat <<'EOF_BODY'
{body}
EOF_BODY
            exit 0;;
          "pr merge")
            exit 0;;
          "issue edit")
            exit 0;;
          *)
            # Default: success, empty stdout. Sufficient for any other
            # incidental call the script makes.
            exit 0;;
        esac
    """))
    stub.chmod(stub.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return stub


def _make_stub_git_push(stub_dir: Path) -> None:
    """Wrap git so `git push` becomes a no-op; everything else falls
    through to the real git binary."""
    real_git = shutil.which("git")
    assert real_git is not None
    wrap = stub_dir / "git"
    wrap.write_text(textwrap.dedent(f"""\
        #!/bin/bash
        if [[ "$1" == "push" ]]; then
            exit 0
        fi
        exec "{real_git}" "$@"
    """))
    wrap.chmod(wrap.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


class CreatePrGuardTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        # Initialise a tiny git repo on a non-master branch.
        subprocess.run(["git", "init", "-q", "-b", "main"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.email", "t@t"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.name", "t"], cwd=self.repo, check=True)
        (self.repo / "f").write_text("x\n")
        subprocess.run(["git", "add", "f"], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=self.repo, check=True)
        subprocess.run(["git", "checkout", "-q", "-b", "agent/12345678"],
                       cwd=self.repo, check=True)
        subprocess.run(["git", "remote", "add", "origin", "/dev/null"],
                       cwd=self.repo, check=True)

        self.stub_dir = self.tmp / "stub-bin"
        self.stub_dir.mkdir()
        _make_stub_git_push(self.stub_dir)

        # Wipe any cached repo info from prior runs that could reference
        # the temp dir's slug (cache key is path-derived).
        toplevel = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                                  cwd=self.repo, capture_output=True, text=True,
                                  check=True).stdout.strip()
        cache_key = toplevel.replace("/", "-")
        for cache in (f"/tmp/pod-repo-cache{cache_key}",
                      f"/tmp/pod-base-branch-cache{cache_key}"):
            try:
                os.unlink(cache)
            except FileNotFoundError:
                pass

    def _run(self, body: str, partial: bool = False):
        _make_stub_gh(self.stub_dir, body=body)
        env = os.environ.copy()
        env["PATH"] = f"{self.stub_dir}:{env['PATH']}"
        env["POD_SESSION_ID"] = "test-session"
        argv = [str(COORDINATION), "create-pr", "999"]
        if partial:
            argv.append("--partial")
        return subprocess.run(argv, cwd=self.repo, env=env,
                              capture_output=True, text=True, timeout=30)

    def test_partial_progress_body_in_full_path_dies(self):
        r = self._run(body="Partial progress on #999\n\nSome details.\n")
        self.assertNotEqual(r.returncode, 0,
            f"expected non-zero exit, got {r.returncode}\nstdout={r.stdout!r}\nstderr={r.stderr!r}")
        combined = r.stdout + r.stderr
        self.assertIn("does not reference", combined)
        self.assertIn("--partial", combined)

    def test_closes_body_in_full_path_succeeds(self):
        # When the body contains `Closes #999`, the guard must not fire.
        r = self._run(body="Closes #999\n\nSome details.\n")
        self.assertEqual(r.returncode, 0,
            f"unexpected non-zero exit\nstdout={r.stdout!r}\nstderr={r.stderr!r}")

    def test_partial_flag_skips_guard(self):
        # The `--partial` flow does not promise to close the issue, so
        # the guard must not fire even with a non-closing body.
        r = self._run(body="Partial progress on #999\n", partial=True)
        self.assertEqual(r.returncode, 0,
            f"unexpected non-zero exit\nstdout={r.stdout!r}\nstderr={r.stderr!r}")


if __name__ == "__main__":
    unittest.main()
