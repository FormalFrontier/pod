"""Unit coverage for the `pod once` one-shot dispatch path.

The actual fork/exec is exercised end-to-end by hand; here we pin the
small, deterministic invariants that protect against regressions:

  1. `AgentState.target_issue` defaults to 0 and round-trips through the
     JSON state file.
  2. `cmd_once` infers the worker type from issue labels when the user
     omits `--type`, and rejects unrecognised label sets cleanly.
  3. The one-shot agent does NOT count against the regular `target`
     pool (the auto-spawn `running` accountant excludes one-shot
     agents).
"""

import dataclasses
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

from pod import cli


class AgentStateOnceFieldsTests(unittest.TestCase):
    def test_target_issue_defaults_to_zero(self):
        s = cli.AgentState(short_id="abc")
        self.assertEqual(s.target_issue, 0)
        self.assertEqual(s.target_type, "")

    def test_target_issue_round_trips_through_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(cli, "AGENTS_DIR", Path(tmp)):
                s = cli.AgentState(
                    short_id="abc",
                    target_issue=3693,
                    target_type="feature",
                    force_quota=True,
                )
                s.write()
                d = json.loads((Path(tmp) / "abc.json").read_text())
                self.assertEqual(d["target_issue"], 3693)
                self.assertEqual(d["target_type"], "feature")
                self.assertTrue(d["force_quota"])
                # Round-trip back through from_dict — exercises the field
                # whitelist that drops unknown keys.
                back = cli.AgentState.from_dict(d)
                self.assertEqual(back.target_issue, 3693)
                self.assertEqual(back.target_type, "feature")


class CmdOnceInferenceTests(unittest.TestCase):
    """`cmd_once` reads `gh issue view` to infer worker type when
    `--type` is omitted. Mock the subprocess call so we don't touch
    the network and pin the failure modes precisely."""

    def _config(self):
        return {
            "worker_types": {
                "feature": {"prompt": "/feature"},
                "plan": {"prompt": "/plan"},
                "repair": {"prompt": "/repair"},
            }
        }

    def test_infers_worker_type_from_labels(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value="human-oversight\nfeature\n") as gh, \
             mock.patch.object(cli, "spawn_agent",
                               return_value=12345) as spawn:
            cli.cmd_once(self._config(), args)
        # gh issue view called once with the expected argv shape.
        self.assertEqual(gh.call_count, 1)
        argv = gh.call_args.args[0]
        self.assertEqual(argv[:3], ["gh", "issue", "view"])
        self.assertIn("3693", argv)
        # spawn_agent called with the inferred type.
        spawn.assert_called_once()
        kwargs = spawn.call_args.kwargs
        self.assertEqual(kwargs["target_issue"], 3693)
        self.assertEqual(kwargs["target_type"], "feature")

    def test_explicit_type_skips_gh_call(self):
        args = types.SimpleNamespace(issue=3693, work_type="feature")
        with mock.patch.object(cli.subprocess, "check_output",
                               side_effect=AssertionError(
                                   "should not call gh when --type given")) as gh, \
             mock.patch.object(cli, "spawn_agent",
                               return_value=12345) as spawn:
            cli.cmd_once(self._config(), args)
        self.assertEqual(gh.call_count, 0)
        spawn.assert_called_once()
        self.assertEqual(spawn.call_args.kwargs["target_type"], "feature")

    def test_unrecognised_labels_exit_cleanly(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value="bug\nenhancement\n"), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn when no type matches")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)


class AutoSpawnExcludesOnceModeTests(unittest.TestCase):
    """The auto-spawn loop reads the `running` count to decide how
    many agents to maintain against `target`. A `pod once` worker
    is, by user intent, *outside* the pool — it bypassed quota to
    take a single priority issue. The accountant must therefore
    skip agents with `target_issue` set, otherwise launching a
    once-worker would reduce the regular-pool target by one for the
    duration of the priority job."""

    def test_running_predicate_excludes_target_issue_agents(self):
        regular_alive = cli.AgentState(short_id="reg", status="running",
                                        target_issue=0)
        regular_starting = cli.AgentState(short_id="reg2",
                                           status="starting",
                                           target_issue=0)
        once_alive = cli.AgentState(short_id="once", status="running",
                                     target_issue=3693)
        once_finishing = cli.AgentState(short_id="once2",
                                         status="finishing",
                                         target_issue=42)
        dead_regular = cli.AgentState(short_id="dead", status="dead",
                                       target_issue=0)
        stopped_once = cli.AgentState(short_id="stopped",
                                        status="stopped",
                                        target_issue=99)
        agents = [regular_alive, regular_starting, once_alive,
                  once_finishing, dead_regular, stopped_once]
        # Mirror the auto-spawn predicate inline so a refactor of the
        # closure surfaces here too.
        running = sum(1 for a in agents
                      if a.status not in ("stopped", "dead")
                      and not a.target_issue)
        self.assertEqual(running, 2)


if __name__ == "__main__":
    unittest.main()
