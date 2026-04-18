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


if __name__ == "__main__":
    unittest.main()
