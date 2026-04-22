import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


class CodexIsolationTests(unittest.TestCase):
    def _config(self):
        return {
            "agent": {
                "backend": "codex",
                "codex": {
                    "model": "gpt-5.4",
                    "isolated_config": True,
                },
            }
        }

    def test_setup_codex_home_is_strict_and_pod_owned(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            worktree = Path(tmp) / "wt"
            real_codex = home / ".codex"
            real_codex.mkdir(parents=True)
            (real_codex / "auth.json").write_text('{"token":"x"}')
            (real_codex / "config.toml").write_text('model = "bad"\n')
            (real_codex / "AGENTS.md").write_text("global agents")
            (real_codex / "history.jsonl").write_text("history")
            (real_codex / "models_cache.json").write_text("{}")
            (real_codex / "plugins").mkdir()
            (real_codex / "rules").mkdir()
            worktree.mkdir()

            with mock.patch("pathlib.Path.home", return_value=home), \
                 mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"):
                codex_home = cli._setup_codex_home(self._config(), str(worktree))

            self.assertIsNotNone(codex_home)
            codex_home = Path(codex_home)
            self.assertTrue((codex_home / "auth.json").is_symlink())
            self.assertEqual(
                (codex_home / "auth.json").resolve(),
                (real_codex / "auth.json").resolve(),
            )

            config_text = (codex_home / "config.toml").read_text()
            self.assertIn('model = "gpt-5.4"', config_text)
            self.assertIn('approval_policy = "never"', config_text)
            self.assertIn('sandbox_mode = "danger-full-access"', config_text)
            self.assertNotEqual(config_text, (real_codex / "config.toml").read_text())

            self.assertTrue((codex_home / "skills").is_dir())
            self.assertFalse((codex_home / "AGENTS.md").exists())
            self.assertFalse((codex_home / "plugins").exists())
            self.assertFalse((codex_home / "rules").exists())
            self.assertFalse((codex_home / "history.jsonl").exists())
            self.assertFalse((codex_home / "models_cache.json").exists())

    def test_setup_codex_home_refreshes_pod_owned_skills_but_preserves_codex_state(self):
        # Pod-owned skills/ must be refreshed from the bundled data each
        # launch; Codex-owned state (plugins/, sqlite dbs, sessions, etc.)
        # must NOT be wiped — previous runs' rollouts and caches should
        # survive.
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            worktree = Path(tmp) / "wt"
            project_dir = Path(tmp) / "project"
            real_codex = home / ".codex"
            real_codex.mkdir(parents=True)
            (real_codex / "auth.json").write_text("{}")
            stale_home = worktree / ".pod-codex-home"
            # Stale pod-owned content (should be refreshed)
            (stale_home / "skills").mkdir(parents=True)
            (stale_home / "skills" / "stale-skill.md").write_text("should go")
            # Codex-owned state that must be preserved across launches
            (stale_home / "plugins").mkdir(parents=True)
            (stale_home / "plugins" / "bundled").write_text("codex-owned")
            (stale_home / "cache").mkdir()
            (stale_home / "cache" / "c").write_text("keep me")
            worktree.mkdir(exist_ok=True)
            project_dir.mkdir(exist_ok=True)

            with mock.patch("pathlib.Path.home", return_value=home), \
                 mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"), \
                 mock.patch.object(cli, "PROJECT_DIR", project_dir):
                codex_home = Path(cli._setup_codex_home(self._config(), str(worktree)))

            # Pod-owned: refreshed
            self.assertTrue((codex_home / "skills").is_dir())
            self.assertFalse((codex_home / "skills" / "stale-skill.md").exists())
            # Codex-owned: preserved
            self.assertTrue((codex_home / "plugins" / "bundled").exists())
            self.assertTrue((codex_home / "cache" / "c").exists())

    def test_setup_codex_home_symlinks_sessions_into_project(self):
        # Codex writes rollouts to $CODEX_HOME/sessions/YYYY/MM/DD/*.jsonl;
        # pod redirects that subtree into the project so rollouts persist
        # across worktrees and agents.
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            worktree = Path(tmp) / "wt"
            project_dir = Path(tmp) / "project"
            real_codex = home / ".codex"
            real_codex.mkdir(parents=True)
            (real_codex / "auth.json").write_text("{}")
            worktree.mkdir()
            project_dir.mkdir()

            with mock.patch("pathlib.Path.home", return_value=home), \
                 mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"), \
                 mock.patch.object(cli, "PROJECT_DIR", project_dir):
                codex_home = Path(cli._setup_codex_home(self._config(), str(worktree)))

            sessions_link = codex_home / "sessions"
            self.assertTrue(sessions_link.is_symlink())
            self.assertEqual(
                sessions_link.resolve(),
                (project_dir / ".pod" / "codex-sessions").resolve(),
            )

    def test_setup_codex_home_preserves_rollouts_across_relaunches(self):
        # A rollout written by a previous launch must still be readable after
        # a fresh _setup_codex_home — the whole point of not wiping the home
        # and symlinking sessions to a project-level dir.
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            worktree = Path(tmp) / "wt"
            project_dir = Path(tmp) / "project"
            (home / ".codex").mkdir(parents=True)
            (home / ".codex" / "auth.json").write_text("{}")
            worktree.mkdir()
            project_dir.mkdir()

            with mock.patch("pathlib.Path.home", return_value=home), \
                 mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"), \
                 mock.patch.object(cli, "PROJECT_DIR", project_dir):
                # First launch
                codex_home = Path(cli._setup_codex_home(self._config(), str(worktree)))
                dated = codex_home / "sessions" / "2026" / "04" / "22"
                dated.mkdir(parents=True)
                rollout = dated / "rollout-2026-04-22T03-00-00-000abcde-0000-0000-0000-000000000000.jsonl"
                rollout.write_text('{"type":"session_meta"}\n')

                # Second launch
                cli._setup_codex_home(self._config(), str(worktree))

                # Rollout should still be there via the project-shared dir
                shared = project_dir / ".pod" / "codex-sessions" / "2026" / "04" / "22"
                self.assertTrue(list(shared.glob("rollout-*.jsonl")))

    def test_install_codex_config_writes_managed_agents_when_repo_has_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            wt = Path(tmp)
            (wt / ".claude").mkdir()
            (wt / ".claude" / "CLAUDE.md").write_text("project claude guidance")

            cli.install_agent_config(str(wt), backend="codex")
            text = (wt / "AGENTS.md").read_text()

            self.assertIn(cli._POD_CODEX_AGENTS_SENTINEL, text)
            self.assertIn("project claude guidance", text)
            self.assertIn("Pod Agent Session", text)

    def test_install_codex_config_appends_to_repo_owned_agents(self):
        with tempfile.TemporaryDirectory() as tmp:
            wt = Path(tmp)
            (wt / "AGENTS.md").write_text("repo owned agents")
            (wt / ".claude").mkdir()
            (wt / ".claude" / "CLAUDE.md").write_text("project claude guidance")

            cli.install_agent_config(str(wt), backend="codex")
            text = (wt / "AGENTS.md").read_text()

            self.assertIn("repo owned agents", text)
            self.assertIn(cli._POD_CODEX_AGENTS_SENTINEL, text)
            self.assertIn("project claude guidance", text)
            self.assertIn("Pod Agent Session", text)

    def test_install_codex_config_is_idempotent_for_repo_owned_agents(self):
        with tempfile.TemporaryDirectory() as tmp:
            wt = Path(tmp)
            (wt / "AGENTS.md").write_text("repo owned agents")
            (wt / ".claude").mkdir()
            (wt / ".claude" / "CLAUDE.md").write_text("project claude guidance")

            cli.install_agent_config(str(wt), backend="codex")
            first = (wt / "AGENTS.md").read_text()
            cli.install_agent_config(str(wt), backend="codex")
            second = (wt / "AGENTS.md").read_text()

            self.assertEqual(first, second)
            self.assertEqual(second.count(cli._POD_CODEX_AGENTS_SENTINEL), 1)

    def test_pod_installed_files_protects_root_agents(self):
        protected = cli._pod_installed_files()
        self.assertIn("AGENTS.md", protected)

    def test_codex_and_claude_command_sets_match(self):
        data = cli._data_dir() / "agent-config"
        claude_commands = sorted(
            p.relative_to(data / "claude" / "commands").as_posix()
            for p in (data / "claude" / "commands").rglob("*")
            if p.is_file()
        )
        codex_commands = sorted(
            p.relative_to(data / "codex" / "commands").as_posix()
            for p in (data / "codex" / "commands").rglob("*")
            if p.is_file()
        )
        self.assertEqual(claude_commands, codex_commands)

    def test_codex_and_claude_skill_sets_match(self):
        data = cli._data_dir() / "agent-config"
        claude_skills = sorted(
            p.relative_to(data / "claude" / "skills").as_posix()
            for p in (data / "claude" / "skills").rglob("*")
            if p.is_file()
        )
        codex_skills = sorted(
            p.relative_to(data / "codex" / "skills").as_posix()
            for p in (data / "codex" / "skills").rglob("*")
            if p.is_file()
        )
        self.assertEqual(claude_skills, codex_skills)


if __name__ == "__main__":
    unittest.main()
