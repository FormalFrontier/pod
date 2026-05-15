"""Tests for the `replan` worker-type dispatch path.

Covers:
- `_filtered_issues` parameterised exclude behaviour
- `_unclaimed_filter` backward-compat wrapper
- `_replan_issues` trust-gate + label-set behaviour
- `_list_replan_count` / `_count_running_replan_agents` shape
- `dispatch_queue_balance` replan short-circuit ordering, lock handling,
  draining fallback, and conditional `list-replan` invocation
"""
from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from pod import cli, coordination


def _completed(returncode: int, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(
        args=[], returncode=returncode, stdout=stdout, stderr=stderr,
    )


def _issue(number: int, labels: list[str], title: str = "x",
           created: str = "2026-01-01T00:00:00Z") -> dict:
    return {
        "number": number,
        "title": title,
        "labels": [{"name": n} for n in labels],
        "createdAt": created,
    }


class FilteredIssuesTests(unittest.TestCase):
    """`_filtered_issues` drops items whose labels intersect `exclude`."""

    def test_replan_exclude_drops_claimed_blocked_has_pr(self):
        items = [
            _issue(1, ["agent-plan", "replan"]),
            _issue(2, ["agent-plan", "replan", "claimed"]),
            _issue(3, ["agent-plan", "replan", "blocked"]),
            _issue(4, ["agent-plan", "replan", "has-pr"]),
            _issue(5, ["agent-plan", "replan", "critical-path"]),
        ]
        out = coordination._filtered_issues(items,
                                            coordination._REPLAN_EXCLUDE)
        nums = [it["number"] for it in out]
        # 5 (critical-path) sorts first; 1 keeps its order; 2/3/4 dropped.
        self.assertEqual(nums, [5, 1])

    def test_replan_exclude_keeps_replan_label(self):
        items = [_issue(1, ["agent-plan", "replan"])]
        out = coordination._filtered_issues(items,
                                            coordination._REPLAN_EXCLUDE)
        self.assertEqual([it["number"] for it in out], [1])

    def test_unclaimed_filter_still_excludes_replan(self):
        items = [
            _issue(1, ["agent-plan"]),
            _issue(2, ["agent-plan", "replan"]),
        ]
        out = coordination._unclaimed_filter(items)
        self.assertEqual([it["number"] for it in out], [1])


class ReplanIssuesTests(unittest.TestCase):
    """`_replan_issues` goes through `_filter_trusted_issues` with the
    `agent-plan + replan` label pair, applies `_REPLAN_EXCLUDE`, and
    never asks for `directive`."""

    def _ctx(self, include_untrusted: bool = False):
        c = mock.MagicMock()
        c.include_untrusted = include_untrusted
        return c

    def test_passes_agent_plan_and_replan_with_limit_500(self):
        captured: list[list[str]] = []

        def fake_filter(args):
            captured.append(list(args))
            return "[]"

        with mock.patch.object(coordination, "_filter_trusted_issues",
                               side_effect=fake_filter):
            coordination._replan_issues(self._ctx())

        self.assertEqual(len(captured), 1)
        args = captured[0]
        self.assertIn("--label", args)
        # Both labels present.
        label_values = [args[i + 1] for i, a in enumerate(args)
                        if a == "--label"]
        self.assertIn("agent-plan", label_values)
        self.assertIn("replan", label_values)
        self.assertNotIn("directive", label_values)
        # Explicit limit (not the gh default).
        self.assertIn("--limit", args)
        self.assertEqual(args[args.index("--limit") + 1], "500")

    def test_excludes_claimed_blocked_has_pr_but_keeps_plain_replan(self):
        items = [
            _issue(1, ["agent-plan", "replan"]),
            _issue(2, ["agent-plan", "replan", "claimed"]),
            _issue(3, ["agent-plan", "replan", "has-pr"]),
        ]
        import json
        with mock.patch.object(coordination, "_filter_trusted_issues",
                               return_value=json.dumps(items)):
            out = coordination._replan_issues(self._ctx())
        self.assertEqual([it["number"] for it in out], [1])

    def test_include_untrusted_passed_through(self):
        captured: list[list[str]] = []
        with mock.patch.object(coordination, "_filter_trusted_issues",
                               side_effect=lambda args: captured.append(args)
                               or "[]"):
            coordination._replan_issues(self._ctx(include_untrusted=True))
        self.assertIn("--include-untrusted", captured[0])


class ListReplanCountTests(unittest.TestCase):
    def test_counts_non_empty_lines(self):
        with mock.patch.object(
            cli, "coordination",
            return_value=_completed(0, "#1 a\n#2 b\n\n#3 c\n"),
        ):
            self.assertEqual(cli._list_replan_count({}), 3)

    def test_returns_zero_on_nonzero_exit(self):
        with mock.patch.object(cli, "coordination",
                               return_value=_completed(1, "")):
            self.assertEqual(cli._list_replan_count({}), 0)

    def test_returns_zero_on_subprocess_failure(self):
        with mock.patch.object(cli, "coordination",
                               side_effect=subprocess.TimeoutExpired(
                                   cmd=["coordination"], timeout=60)):
            self.assertEqual(cli._list_replan_count({}), 0)


class CountRunningReplanAgentsTests(unittest.TestCase):
    def _agent(self, worker_type: str, status: str):
        a = mock.MagicMock()
        a.worker_type = worker_type
        a.status = status
        return a

    def test_counts_only_live_replan_agents(self):
        agents = [
            self._agent("replan", "running"),
            self._agent("replan", "dead"),
            self._agent("replan", "killed"),
            self._agent("plan", "running"),
            self._agent("repair", "running"),
            self._agent("replan", "starting"),
        ]
        self.assertEqual(cli._count_running_replan_agents(agents), 2)

    def test_empty_list(self):
        self.assertEqual(cli._count_running_replan_agents([]), 0)


class DispatchReplanShortCircuitTests(unittest.TestCase):
    """Order and lock-handling for the replan short-circuit in
    `dispatch_queue_balance`."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        self._patches = [
            mock.patch.object(cli, "TARGET_FILE", root / "target"),
            mock.patch.object(cli, "PLANNER_TARGET_FILE",
                              root / "planner-target"),
            mock.patch.object(cli, "PLANNER_MIN_QUEUE_FILE",
                              root / "planner-min-queue"),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)
        cli.write_target(3)

    def _worker_types(self, with_replan: bool = True):
        wt: dict = {
            "plan": {"lock": "planner"},
            "work": {},
            "repair": {},
        }
        if with_replan:
            wt["replan"] = {"lock": "planner"}
        return wt

    def test_replan_dispatched_when_candidates_and_lock_free(self):
        lock_calls: list[tuple] = []

        def fake_coord(config, *args, **kw):
            lock_calls.append(args)
            if args == ("lock-planner",):
                return _completed(0)
            raise AssertionError(f"unexpected coordination call: {args}")

        with mock.patch.object(cli, "_list_pr_repair_count", return_value=0), \
             mock.patch.object(cli, "_list_replan_count", return_value=1), \
             mock.patch.object(cli, "_count_running_replan_agents",
                               return_value=0), \
             mock.patch.object(cli, "coordination", side_effect=fake_coord):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=0,
                worker_types=self._worker_types(),
            )
        self.assertEqual(chosen, "replan")
        # Exactly one lock attempt — no double-acquire.
        self.assertEqual(
            sum(1 for a in lock_calls if a == ("lock-planner",)), 1)

    def test_lock_held_empty_queue_returns_none_and_one_lock_attempt(self):
        lock_calls: list[tuple] = []

        def fake_coord(config, *args, **kw):
            lock_calls.append(args)
            if args == ("lock-planner",):
                return _completed(1)  # held by someone else
            raise AssertionError(f"unexpected coordination call: {args}")

        with mock.patch.object(cli, "_list_pr_repair_count", return_value=0), \
             mock.patch.object(cli, "_list_replan_count", return_value=1), \
             mock.patch.object(cli, "_count_running_replan_agents",
                               return_value=0), \
             mock.patch.object(cli, "coordination", side_effect=fake_coord):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=0,
                worker_types=self._worker_types(),
            )
        self.assertIsNone(chosen)
        # Must not call `lock-planner` again from the filling iteration.
        self.assertEqual(
            sum(1 for a in lock_calls if a == ("lock-planner",)), 1)

    def test_lock_held_with_draining_returns_draining_choice(self):
        lock_calls: list[tuple] = []

        def fake_coord(config, *args, **kw):
            lock_calls.append(args)
            if args == ("lock-planner",):
                return _completed(1)
            raise AssertionError(f"unexpected coordination call: {args}")

        with mock.patch.object(cli, "_list_pr_repair_count", return_value=0), \
             mock.patch.object(cli, "_list_replan_count", return_value=1), \
             mock.patch.object(cli, "_count_running_replan_agents",
                               return_value=0), \
             mock.patch.object(cli, "_choose_draining", return_value="work"), \
             mock.patch.object(cli, "coordination", side_effect=fake_coord):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=5,
                worker_types=self._worker_types(),
            )
        self.assertEqual(chosen, "work")
        self.assertEqual(
            sum(1 for a in lock_calls if a == ("lock-planner",)), 1)

    def test_repair_wins_when_both_have_candidates(self):
        # No `lock-planner` should ever be attempted: repair short-circuits
        # before the replan block.
        with mock.patch.object(cli, "_list_pr_repair_count", return_value=1), \
             mock.patch.object(cli, "_count_running_repair_agents",
                               return_value=0), \
             mock.patch.object(cli, "_list_replan_count",
                               side_effect=AssertionError(
                                   "list-replan must not be called when "
                                   "repair has candidates")), \
             mock.patch.object(cli, "coordination",
                               side_effect=AssertionError(
                                   "no coordination call expected")):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=0,
                worker_types=self._worker_types(),
            )
        self.assertEqual(chosen, "repair")

    def test_no_replan_worker_type_skips_list_replan(self):
        # Old hex config (no `[worker_types.replan]`) → the new short-circuit
        # is a no-op and `list-replan` is never invoked.
        with mock.patch.object(cli, "_list_pr_repair_count", return_value=0), \
             mock.patch.object(cli, "_list_replan_count",
                               side_effect=AssertionError(
                                   "list-replan must not be called when "
                                   "worker_types.replan is absent")), \
             mock.patch.object(cli, "_choose_draining", return_value="work"):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=5,
                worker_types=self._worker_types(with_replan=False),
            )
        self.assertEqual(chosen, "work")

    def test_replan_skipped_when_running_count_meets_candidates(self):
        # Already a /replan agent for the only candidate — don't spawn another.
        # Fall through to the regular path; draining is available.
        lock_calls: list[tuple] = []

        def fake_coord(config, *args, **kw):
            lock_calls.append(args)
            # Critical-path override may run and consult critical-path-depth.
            if args[0] == "critical-path-depth":
                return _completed(0, "0")
            # Any planner-lock attempt is a regression for this test.
            if args == ("lock-planner",):
                raise AssertionError(
                    "must not attempt planner lock when running >= candidates")
            return _completed(0, "")

        with mock.patch.object(cli, "_list_pr_repair_count", return_value=0), \
             mock.patch.object(cli, "_list_replan_count", return_value=1), \
             mock.patch.object(cli, "_count_running_replan_agents",
                               return_value=1), \
             mock.patch.object(cli, "_choose_draining", return_value="work"), \
             mock.patch.object(cli, "coordination", side_effect=fake_coord):
            chosen = cli.dispatch_queue_balance(
                config={}, queue_depth=5,
                worker_types=self._worker_types(),
            )
        self.assertEqual(chosen, "work")
        self.assertNotIn(("lock-planner",), lock_calls)


if __name__ == "__main__":
    unittest.main()
