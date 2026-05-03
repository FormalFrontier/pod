"""Tests for auto-mode backend selection."""

import subprocess
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


def _quota_result(returncode: int, stdout: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")


def _config(backend: str, prefer: str = "codex"):
    return {
        "agent": {
            "backend": backend,
            "auto": {"prefer": prefer},
            "claude": {
                "model": "opus",
                "quota_check": "/usr/bin/false",
                "quota_check_required": True,
            },
            "codex": {
                "model": "gpt-5.4",
                "quota_check": "/usr/bin/false",
                "quota_check_required": True,
            },
        }
    }


class BackendCfgTests(unittest.TestCase):
    def test_default_uses_configured_backend(self):
        cfg = _config("claude")
        self.assertEqual(cli._backend_cfg(cfg, "model"), "opus")

    def test_explicit_backend_overrides(self):
        cfg = _config("claude")
        self.assertEqual(cli._backend_cfg(cfg, "model", backend="codex"), "gpt-5.4")

    def test_explicit_backend_works_in_auto_mode(self):
        cfg = _config("auto")
        self.assertEqual(cli._backend_cfg(cfg, "model", backend="codex"), "gpt-5.4")
        self.assertEqual(cli._backend_cfg(cfg, "model", backend="claude"), "opus")


class AutoBackendOrderTests(unittest.TestCase):
    def test_default_prefers_codex(self):
        # When prefer key is missing, default is "codex"
        cfg = {"agent": {"backend": "auto", "auto": {}}}
        self.assertEqual(cli._auto_backend_order(cfg), ["codex", "claude"])

    def test_prefer_claude(self):
        cfg = _config("auto", prefer="claude")
        self.assertEqual(cli._auto_backend_order(cfg), ["claude", "codex"])

    def test_invalid_prefer_falls_back_to_codex(self):
        cfg = _config("auto", prefer="bogus")
        self.assertEqual(cli._auto_backend_order(cfg), ["codex", "claude"])


class SelectAvailableBackendTests(unittest.TestCase):
    def test_fixed_backend_returns_self_when_quota_ok(self):
        cfg = _config("claude")
        with mock.patch.object(cli, "_backend_available", return_value=True):
            self.assertEqual(cli.select_available_backend(cfg), "claude")

    def test_fixed_backend_returns_none_when_no_quota(self):
        cfg = _config("codex")
        with mock.patch.object(cli, "_backend_available", return_value=False):
            self.assertIsNone(cli.select_available_backend(cfg))

    def test_auto_returns_preferred_when_available(self):
        cfg = _config("auto", prefer="codex")
        # Both available — preferred wins
        with mock.patch.object(cli, "_backend_available", return_value=True) as m:
            self.assertEqual(cli.select_available_backend(cfg), "codex")
            # Only the preferred backend is queried (short-circuit)
            self.assertEqual(m.call_count, 1)
            self.assertEqual(m.call_args.args[1], "codex")

    def test_auto_falls_back_to_other_when_preferred_unavailable(self):
        cfg = _config("auto", prefer="codex")
        seen: list[str] = []

        def avail(_cfg, backend, _claude_dir=None):
            seen.append(backend)
            return backend == "claude"

        with mock.patch.object(cli, "_backend_available", side_effect=avail):
            self.assertEqual(cli.select_available_backend(cfg), "claude")
        self.assertEqual(seen, ["codex", "claude"])

    def test_auto_returns_none_when_neither_available(self):
        cfg = _config("auto")
        with mock.patch.object(cli, "_backend_available", return_value=False):
            self.assertIsNone(cli.select_available_backend(cfg))

    def test_auto_force_returns_preferred_without_calling_quota(self):
        cfg = _config("auto", prefer="claude")
        with mock.patch.object(cli, "_backend_available") as m:
            self.assertEqual(cli.select_available_backend(cfg, force=True), "claude")
            m.assert_not_called()

    def test_fixed_force_returns_self_without_calling_quota(self):
        cfg = _config("codex")
        with mock.patch.object(cli, "_backend_available") as m:
            self.assertEqual(cli.select_available_backend(cfg, force=True), "codex")
            m.assert_not_called()


class BackendAvailableTests(unittest.TestCase):
    def _cfg(self):
        return _config("auto")

    def test_returns_true_on_exit_zero_with_matching_model(self):
        cfg = self._cfg()
        with mock.patch.object(cli.subprocess, "run",
                               return_value=_quota_result(0, "gpt-5.4\n")), \
             mock.patch.object(cli, "_reload_config_value", return_value="gpt-5.4"), \
             mock.patch.object(cli, "_model_tier", return_value=1):
            self.assertTrue(cli._backend_available(cfg, "codex"))

    def test_returns_false_on_nonzero_exit(self):
        cfg = self._cfg()
        with mock.patch.object(cli.subprocess, "run",
                               return_value=_quota_result(1)):
            self.assertFalse(cli._backend_available(cfg, "codex"))

    def test_exit_two_returns_false_when_required(self):
        cfg = self._cfg()  # quota_check_required=True
        with mock.patch.object(cli.subprocess, "run",
                               return_value=_quota_result(2)):
            self.assertFalse(cli._backend_available(cfg, "codex"))

    def test_exit_two_returns_true_when_not_required(self):
        cfg = self._cfg()
        cfg["agent"]["codex"]["quota_check_required"] = False
        with mock.patch.object(cli.subprocess, "run",
                               return_value=_quota_result(2)):
            self.assertTrue(cli._backend_available(cfg, "codex"))

    def test_uses_per_backend_quota_check(self):
        """Each backend's own quota_check command is invoked, not the global."""
        cfg = self._cfg()
        cfg["agent"]["claude"]["quota_check"] = "/path/to/claude-check"
        cfg["agent"]["codex"]["quota_check"] = "/path/to/codex-check"
        with mock.patch.object(cli.subprocess, "run",
                               return_value=_quota_result(1)) as run, \
             mock.patch.object(cli, "resolve_claude_credential",
                               return_value={"accountLabel": "?"}):
            cli._backend_available(cfg, "claude")
            self.assertEqual(run.call_args.args[0][0], "/path/to/claude-check")
            cli._backend_available(cfg, "codex")
            self.assertEqual(run.call_args.args[0][0], "/path/to/codex-check")

    def test_empty_quota_check_returns_true(self):
        cfg = self._cfg()
        cfg["agent"]["codex"]["quota_check"] = ""
        self.assertTrue(cli._backend_available(cfg, "codex"))


class GetIsolatedConfigDirTests(unittest.TestCase):
    def test_returns_path_for_claude(self):
        cfg = {"agent": {"backend": "claude",
                          "claude": {"isolated_config": True}}}
        self.assertEqual(cli.get_isolated_config_dir(cfg), cli.ISOLATED_CONFIG_DIR)

    def test_returns_path_for_auto(self):
        cfg = {"agent": {"backend": "auto",
                          "claude": {"isolated_config": True}}}
        self.assertEqual(cli.get_isolated_config_dir(cfg), cli.ISOLATED_CONFIG_DIR)

    def test_returns_none_for_codex(self):
        cfg = {"agent": {"backend": "codex",
                          "claude": {"isolated_config": True}}}
        self.assertIsNone(cli.get_isolated_config_dir(cfg))

    def test_returns_none_when_isolation_disabled(self):
        cfg = {"agent": {"backend": "auto",
                          "claude": {"isolated_config": False}}}
        self.assertIsNone(cli.get_isolated_config_dir(cfg))


class InstructionFileWarningTests(unittest.TestCase):
    def test_auto_requires_both(self):
        with mock.patch("pathlib.Path.exists", return_value=False), \
             mock.patch("pathlib.Path.is_symlink", return_value=False):
            self.assertIsNone(cli._instruction_file_warning(Path("/r"), "auto"))

    def test_auto_warns_when_only_agents_md(self):
        # AGENTS.md exists, .claude/CLAUDE.md does not
        def exists_side_effect(self):
            return self.name == "AGENTS.md"

        with mock.patch("pathlib.Path.exists", new=exists_side_effect), \
             mock.patch("pathlib.Path.is_symlink", return_value=False):
            warning = cli._instruction_file_warning(Path("/r"), "auto")
        self.assertIsNotNone(warning)
        self.assertIn("AGENTS.md but no", warning)

    def test_auto_warns_when_only_claude_md(self):
        def exists_side_effect(self):
            return self.name == "CLAUDE.md"

        with mock.patch("pathlib.Path.exists", new=exists_side_effect), \
             mock.patch("pathlib.Path.is_symlink", return_value=False):
            warning = cli._instruction_file_warning(Path("/r"), "auto")
        self.assertIsNotNone(warning)
        self.assertIn("CLAUDE.md but no AGENTS.md", warning)

    def test_claude_does_not_warn_when_only_claude_md(self):
        def exists_side_effect(self):
            return self.name == "CLAUDE.md"

        with mock.patch("pathlib.Path.exists", new=exists_side_effect), \
             mock.patch("pathlib.Path.is_symlink", return_value=False):
            self.assertIsNone(cli._instruction_file_warning(Path("/r"), "claude"))

    def test_codex_does_not_warn_when_only_agents_md(self):
        def exists_side_effect(self):
            return self.name == "AGENTS.md"

        with mock.patch("pathlib.Path.exists", new=exists_side_effect), \
             mock.patch("pathlib.Path.is_symlink", return_value=False):
            self.assertIsNone(cli._instruction_file_warning(Path("/r"), "codex"))


class WorkerPromptBackendArgTests(unittest.TestCase):
    def test_default_uses_configured_backend(self):
        cfg = {"agent": {"backend": "claude"},
               "worker_types": {"work": {"prompt": "/work"}}}
        self.assertEqual(cli._worker_prompt(cfg, "work"), "/work")

    def test_explicit_codex_resolves_template_file(self):
        cfg = {"agent": {"backend": "claude"},
               "worker_types": {"work": {"prompt": "/work"}}}
        # In auto mode, an iteration may resolve to codex even though the
        # configured backend is "auto" — _worker_prompt must honour the
        # backend arg.
        with mock.patch.object(cli, "_data_dir") as data_dir:
            fake_template = mock.MagicMock()
            fake_template.is_file.return_value = True
            fake_template.read_text.return_value = "codex template body"
            data_dir.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value.__truediv__.return_value = fake_template
            result = cli._worker_prompt(cfg, "work", backend="codex")
        self.assertEqual(result, "codex template body")


class CheckQuotaShimTests(unittest.TestCase):
    def test_returns_true_when_select_returns_backend(self):
        cfg = _config("auto")
        with mock.patch.object(cli, "select_available_backend", return_value="codex"):
            self.assertTrue(cli.check_quota(cfg))

    def test_returns_false_when_select_returns_none(self):
        cfg = _config("auto")
        with mock.patch.object(cli, "select_available_backend", return_value=None):
            self.assertFalse(cli.check_quota(cfg))


if __name__ == "__main__":
    unittest.main()
