import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


class StateFileTests(unittest.TestCase):
    def test_write_target_is_self_documenting_and_round_trips(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "target"
            with mock.patch.object(cli, "TARGET_FILE", target):
                cli.write_target(8)
                self.assertEqual(cli.read_target(), 8)
                self.assertEqual(
                    target.read_text(),
                    "# User target agent count.\n"
                    "# pod tries to keep this many agents running.\n"
                    "8\n",
                )

    def test_reader_ignores_comments_and_blank_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "planner-target"
            path.write_text(
                "# Planner advisory.\n"
                "\n"
                "# Effective target is min(target, planner-target).\n"
                "3\n"
            )
            self.assertEqual(cli._read_commented_int(path), 3)

    def test_reader_rejects_invalid_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "planner-min-queue"
            path.write_text("# comment only\nnot-an-int\n")
            self.assertIsNone(cli._read_commented_int(path))

    def test_instruction_file_warning_when_claude_exists_without_agents_for_codex(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".claude").mkdir()
            (root / ".claude" / "CLAUDE.md").write_text("x")
            warning = cli._instruction_file_warning(root, "codex")
            self.assertIsNotNone(warning)
            self.assertIn("no AGENTS.md", warning)

    def test_instruction_file_warning_suppressed_when_claude_exists_for_claude_backend(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".claude").mkdir()
            (root / ".claude" / "CLAUDE.md").write_text("x")
            self.assertIsNone(cli._instruction_file_warning(root, "claude"))

    def test_instruction_file_warning_when_agents_not_symlinked(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".claude").mkdir()
            (root / ".claude" / "CLAUDE.md").write_text("x")
            (root / "AGENTS.md").write_text("y")
            warning = cli._instruction_file_warning(root, "codex")
            self.assertIsNotNone(warning)
            self.assertIn("not symlinked together", warning)

    def test_instruction_file_warning_suppressed_for_canonical_symlink(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / ".claude").mkdir()
            (root / ".claude" / "CLAUDE.md").write_text("x")
            (root / "AGENTS.md").symlink_to(".claude/CLAUDE.md")
            self.assertIsNone(cli._instruction_file_warning(root, "codex"))


if __name__ == "__main__":
    unittest.main()
