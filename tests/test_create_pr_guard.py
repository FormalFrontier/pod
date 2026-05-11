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
    `cmd_create_pr` makes before the guard fires.

    Built without `textwrap.dedent` because the legacy bash version
    tolerated an indented shebang (the shell falls back to running the
    script via /bin/sh on exec-format error), but Python's
    `subprocess.run` does not, and the post-port coordination dispatcher
    raises `OSError(Errno 8: Exec format error)`. Writing the script
    with explicit `\\n` keeps the shebang at byte 0 regardless of the
    interpolated `body` content.
    """
    stub = stub_dir / "gh"
    script = (
        "#!/bin/bash\n"
        "# Minimal gh stub for cmd_create_pr guard test. Routes by argv shape.\n"
        "set -e\n"
        "case \"$1 $2\" in\n"
        "  \"repo view\")\n"
        "    # `gh repo view --json nameWithOwner -q .nameWithOwner` etc.\n"
        "    for arg in \"$@\"; do\n"
        "      case \"$arg\" in\n"
        "        .nameWithOwner) echo \"owner/repo\"; exit 0;;\n"
        "        .defaultBranchRef.name) echo \"main\"; exit 0;;\n"
        "      esac\n"
        "    done\n"
        "    exit 0;;\n"
        "  \"pr list\")\n"
        f"    echo \"{existing_pr_number}\"\n"
        "    exit 0;;\n"
        "  \"pr view\")\n"
        "    cat <<'EOF_BODY'\n"
        f"{body}\n"
        "EOF_BODY\n"
        "    exit 0;;\n"
        "  \"pr merge\")\n"
        "    exit 0;;\n"
        "  \"issue edit\")\n"
        "    exit 0;;\n"
        "  *)\n"
        "    exit 0;;\n"
        "esac\n"
    )
    stub.write_text(script)
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

        # Pre-seed the repo / base-branch caches so coordination doesn't
        # try to hit the real `api.github.com` looking up default_branch
        # (the bash version went through `gh repo view`, our port routes
        # the default-branch lookup through the layer's httpx client which
        # WILL touch the network).
        toplevel = subprocess.run(["git", "rev-parse", "--show-toplevel"],
                                  cwd=self.repo, capture_output=True, text=True,
                                  check=True).stdout.strip()
        cache_key = toplevel.replace("/", "-")
        Path(f"/tmp/pod-repo-cache{cache_key}").write_text("owner/repo\n")
        Path(f"/tmp/pod-base-branch-cache{cache_key}").write_text("main\n")
        # Also pre-seed the labels-ensured flag so we don't try to
        # bootstrap labels (which would issue a dozen gh stub calls).
        Path(f"/tmp/pod-labels-ensured-owner-repo").touch()
        # Clean up the seeded files after the test.
        def _cleanup_caches():
            for p in (f"/tmp/pod-repo-cache{cache_key}",
                      f"/tmp/pod-base-branch-cache{cache_key}",
                      "/tmp/pod-labels-ensured-owner-repo"):
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass
        self.addCleanup(_cleanup_caches)

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
