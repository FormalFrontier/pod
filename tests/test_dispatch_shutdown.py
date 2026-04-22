"""Regression tests for issue #27.

When the planner calls `coordination return-to-human`, three state files are
written: `target=0`, `planner-target=0`, `planner-min-queue=1` (or 0).  The
dispatcher must then refuse to hand new prompts to the running agent.
Previously, `dispatch_queue_balance` clamped `min_queue` to 1 and never
consulted `get_effective_target()`, so an agent kept being dispatched as
`plan` indefinitely.
"""
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


class DispatchHonoursReturnToHumanTests(unittest.TestCase):
    """`dispatch_queue_balance` must return None when shutdown is requested."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self._patches = [
            mock.patch.object(cli, "TARGET_FILE", root / "target"),
            mock.patch.object(cli, "PLANNER_TARGET_FILE", root / "planner-target"),
            mock.patch.object(cli, "PLANNER_MIN_QUEUE_FILE", root / "planner-min-queue"),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def _worker_types(self):
        # Mirrors the default `plan` (filling) + `work` (draining) shape that
        # the planner-driven shutdown loop exercises.
        return {
            "plan": {"lock": "planner"},
            "work": {},
        }

    def test_effective_target_zero_returns_none_even_with_empty_queue(self):
        cli.write_target(0)
        cli._write_commented_int(cli.PLANNER_TARGET_FILE, 0,
                                  ["Planner-recommended target."])
        cli._write_commented_int(cli.PLANNER_MIN_QUEUE_FILE, 1,
                                  ["Planner-recommended min queue."])

        chosen = cli.dispatch_queue_balance(
            config={}, queue_depth=0, worker_types=self._worker_types(),
        )
        self.assertIsNone(chosen,
            "dispatch must not return 'plan' when planner asked for shutdown")

    def test_planner_min_queue_zero_skips_filling_branch_when_draining_has_work(self):
        # Belt-and-braces: even without target=0, planner-min-queue=0 must
        # disable the queue-low filling branch.  Previously `max(1, …)`
        # clamped the planner's value back to 1, so queue_depth=0 < 1 always
        # entered the filling branch and dispatched `plan`, even when the
        # draining branch had work to do.
        cli.write_target(3)
        cli._write_commented_int(cli.PLANNER_MIN_QUEUE_FILE, 0,
                                  ["Planner-recommended min queue."])

        # `_choose_draining` returns a worker name → that's what dispatch
        # should pick.  Pre-fix it would have picked `plan` first.  Post-fix
        # the filling branch is gated, so we fall through to draining.
        with mock.patch.object(cli, "_choose_draining", return_value="work"):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=0, worker_types=self._worker_types(),
            )
        self.assertEqual(chosen, "work",
            "planner-min-queue=0 must skip the queue-low filling branch")

    def test_target_zero_short_circuits_before_min_queue_logic(self):
        # When target=0 is set without planner files, the dispatch should still
        # return None — the planner's request is the user's target=0 alone.
        cli.write_target(0)
        # Force the (would-be-broken) min_queue path to throw if ever reached.
        with mock.patch.object(cli, "read_planner_min_queue",
                                side_effect=AssertionError("min_queue must not be consulted")):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=0, worker_types=self._worker_types(),
            )
        self.assertIsNone(chosen)

    def test_round_robin_honours_effective_target_zero(self):
        # round_robin doesn't have a min_queue concept, so the bug presents
        # differently: it just keeps cycling worker types forever.  Same fix
        # required.  Pre-fix: returns "plan" (or "work").  Post-fix: None.
        cli.write_target(0)
        with mock.patch.object(cli, "coordination",
                                side_effect=AssertionError("must not attempt lock when target=0")):
            chosen = cli.dispatch_round_robin(
                config={}, queue_depth=0, worker_types=self._worker_types(),
            )
        self.assertIsNone(chosen)

    def test_custom_dispatch_honours_effective_target_zero(self):
        # Custom dispatch must not even invoke the script when target=0 —
        # there's nothing it could legitimately return that we'd want to act on.
        cli.write_target(0)
        with mock.patch("subprocess.run",
                         side_effect=AssertionError("must not invoke script when target=0")):
            chosen = cli.dispatch_custom(
                config={"dispatch": {"strategy": "/nonexistent/script"}},
                queue_depth=0, worker_types=self._worker_types(),
            )
        self.assertIsNone(chosen)


class GetEffectiveTargetTests(unittest.TestCase):
    """Sanity check on the helper used by the new dispatch guard."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self._patches = [
            mock.patch.object(cli, "TARGET_FILE", root / "target"),
            mock.patch.object(cli, "PLANNER_TARGET_FILE", root / "planner-target"),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def test_returns_zero_when_both_targets_zero(self):
        cli.write_target(0)
        cli._write_commented_int(cli.PLANNER_TARGET_FILE, 0, ["x"])
        self.assertEqual(cli.get_effective_target(), 0)

    def test_returns_zero_when_user_target_zero(self):
        cli.write_target(0)
        # No planner file.
        self.assertEqual(cli.get_effective_target(), 0)


if __name__ == "__main__":
    unittest.main()
