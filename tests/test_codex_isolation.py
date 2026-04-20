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

    def test_setup_codex_home_removes_stale_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp) / "home"
            worktree = Path(tmp) / "wt"
            real_codex = home / ".codex"
            real_codex.mkdir(parents=True)
            (real_codex / "auth.json").write_text("{}")
            stale_home = worktree / ".pod-codex-home"
            (stale_home / "plugins").mkdir(parents=True)
            (stale_home / "plugins" / "bad").write_text("stale")
            worktree.mkdir(exist_ok=True)

            with mock.patch("pathlib.Path.home", return_value=home), \
                 mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"):
                codex_home = Path(cli._setup_codex_home(self._config(), str(worktree)))

            self.assertFalse((codex_home / "plugins").exists())
            self.assertTrue((codex_home / "skills").is_dir())

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
