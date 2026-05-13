"""Unit coverage for the `pod once` one-shot dispatch path.

The actual fork/exec is exercised end-to-end by hand; here we pin the
small, deterministic invariants that protect against regressions:

  * `AgentState.target_issue` / `target_type` round-trip through the
    JSON state file.
  * The `_is_regular_agent` predicate excludes one-shot agents from
    every `target` accountant (auto-spawn, `cmd_add`, `cmd_finish`,
    `cmd_kill`).
  * `_abort_one_shot_iteration` flips `state.finishing` only for
    one-shot agents — regular agents continue their normal retry/backoff.
  * `cmd_once` preflight (open/state, not-yet-claimed, labels matched
    against worker types) refuses every operator-error case with a
    clean exit before spawning.
  * `_once_prompt` returns the `/once` slash form for Claude and reads
    the codex template body for Codex.
"""

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


class IsRegularAgentTests(unittest.TestCase):
    """`_is_regular_agent` is the shared predicate behind every
    `target` accountant. The fix for Codex's MEDIUM #4 finding made
    it explicit so `cmd_add`, `cmd_finish`, `cmd_kill`, and the
    auto-spawn loop all agree on which agents count toward `target`."""

    def test_regular_running_agent_counts(self):
        a = cli.AgentState(short_id="r", status="running")
        self.assertTrue(cli._is_regular_agent(a))

    def test_dead_or_stopped_excluded(self):
        for status in ("stopped", "dead"):
            a = cli.AgentState(short_id="x", status=status)
            self.assertFalse(cli._is_regular_agent(a),
                             f"status={status!r} should not count")

    def test_one_shot_excluded_even_when_running(self):
        a = cli.AgentState(short_id="o", status="running",
                           target_issue=3693)
        self.assertFalse(cli._is_regular_agent(a))

    def test_one_shot_excluded_when_finishing(self):
        # A one-shot agent in `finishing` would otherwise look alive,
        # but it never occupied a `target` slot to begin with.
        a = cli.AgentState(short_id="o", status="finishing",
                           target_issue=3693)
        self.assertFalse(cli._is_regular_agent(a))


class AbortOneShotIterationTests(unittest.TestCase):
    def test_no_op_for_regular_agent(self):
        a = cli.AgentState(short_id="reg")
        self.assertFalse(a.finishing)
        cli._abort_one_shot_iteration(a, "test")
        self.assertFalse(a.finishing,
                         "regular agent must continue iterating")

    def test_marks_one_shot_finishing(self):
        a = cli.AgentState(short_id="o", target_issue=3693)
        self.assertFalse(a.finishing)
        cli._abort_one_shot_iteration(a, "worktree setup failed")
        self.assertTrue(a.finishing,
                        "one-shot agent must exit on iteration failure")


class CmdOnceTests(unittest.TestCase):
    """`cmd_once` preflight invariants. We mock `subprocess.check_output`
    so we never touch the network or fork; the goal is to pin every
    operator-error refusal mode (no PR exit code, no half-spawned
    worker)."""

    def _config(self):
        return {
            "worker_types": {
                "feature": {"prompt": "/feature"},
                "plan": {"prompt": "/plan"},
                "repair": {"prompt": "/repair"},
            }
        }

    def _gh_view(self, labels, state="OPEN", title="x"):
        """Build a `gh issue view --json labels,state,title` payload."""
        return json.dumps({
            "labels": [{"name": n} for n in labels],
            "state": state,
            "title": title,
        })

    def test_infers_worker_type_from_labels(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(
                                   ["human-oversight", "feature"])) as gh, \
             mock.patch.object(cli, "spawn_agent",
                               return_value=12345) as spawn:
            cli.cmd_once(self._config(), args)
        argv = gh.call_args.args[0]
        self.assertEqual(argv[:3], ["gh", "issue", "view"])
        self.assertIn("3693", argv)
        self.assertIn("labels,state,title", argv)
        spawn.assert_called_once()
        kwargs = spawn.call_args.kwargs
        self.assertEqual(kwargs["target_issue"], 3693)
        self.assertEqual(kwargs["target_type"], "feature")

    def test_explicit_type_still_preflights(self):
        """Even with `--type` explicit we read the issue once to
        check it is open + unclaimed. We do NOT skip the gh call."""
        args = types.SimpleNamespace(issue=3693, work_type="feature")
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(["feature"])) as gh, \
             mock.patch.object(cli, "spawn_agent",
                               return_value=12345) as spawn:
            cli.cmd_once(self._config(), args)
        self.assertEqual(gh.call_count, 1)
        spawn.assert_called_once()
        self.assertEqual(spawn.call_args.kwargs["target_type"], "feature")

    def test_unknown_explicit_type_exits_cleanly(self):
        args = types.SimpleNamespace(issue=3693, work_type="frobnicate")
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(["feature"])), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn for unknown --type")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)

    def test_unrecognised_labels_exit_cleanly(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(
                                   ["bug", "enhancement"])), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn when no type matches")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)

    def test_falls_back_to_work_when_configured(self):
        """When no label matches a worker_type but `work` is configured,
        `pod once` defaults to `work` — `human-oversight`-only issues
        (the common case for hand-written work items) shouldn't require
        `--type work` explicitly."""
        config = {
            "worker_types": {
                "work": {"prompt": "/work"},
                "plan": {"prompt": "/plan"},
                "repair": {"prompt": "/repair"},
            }
        }
        args = types.SimpleNamespace(issue=3698, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(
                                   ["human-oversight"])), \
             mock.patch.object(cli, "spawn_agent",
                               return_value=12345) as spawn:
            cli.cmd_once(config, args)
        spawn.assert_called_once()
        self.assertEqual(spawn.call_args.kwargs["target_type"], "work")

    def test_closed_issue_rejected(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(
                                   ["feature"], state="CLOSED")), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn for closed issue")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)

    def test_already_claimed_issue_rejected(self):
        """Codex MEDIUM #3: the preflight must catch a `claimed`
        label *before* spawning. Otherwise the agent creates a
        worktree and registers a branch before discovering the
        race, which is cleanup-able but not 'without starting work'
        in the operational sense."""
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value=self._gh_view(
                                   ["human-oversight", "feature",
                                    "claimed"])), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn when claimed")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)

    def test_unparseable_gh_output_exits_cleanly(self):
        args = types.SimpleNamespace(issue=3693, work_type=None)
        with mock.patch.object(cli.subprocess, "check_output",
                               return_value="not json"), \
             mock.patch.object(cli, "spawn_agent",
                               side_effect=AssertionError(
                                   "should not spawn for bad gh output")):
            with self.assertRaises(SystemExit) as ctx:
                cli.cmd_once(self._config(), args)
            self.assertEqual(ctx.exception.code, 2)


class OncePromptTests(unittest.TestCase):
    def test_claude_returns_slash_form(self):
        self.assertEqual(cli._once_prompt("claude"), "/once")

    def test_codex_reads_template_body(self):
        prompt = cli._once_prompt("codex")
        # Must NOT just be the slash form — Codex has no slash command
        # system. Either the template is found (body) or we log a
        # warning and fall back to `/once`; the template ships with
        # the package so the first path is the expected one.
        self.assertNotEqual(prompt, "/once",
                            "codex backend should resolve to template body")
        self.assertIn("one-shot", prompt.lower())
        self.assertIn("issue number", prompt.lower())


if __name__ == "__main__":
    unittest.main()
