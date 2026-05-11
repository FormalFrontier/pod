"""Regression tests for the repair-agent bookkeeping bugs that caused
unbounded dispatch loops on closed issues.

Bug 1: Both Codex and Claude session parsers scanned ALL command output
for the substring `Claimed issue #(\\d+)` and set `state.claimed_issue`
to the matched number. The pattern matched historical mentions in
unrelated output (`git log`, `coordination orient`, `gh issue view`,
etc.), and applied even when the agent's `worker_type` was `repair` —
which doesn't claim issues at all. Result: a stale `claimed_issue`
pointing to some long-closed issue would be re-asserted on every
command execution.

Bug 2: The end-of-session cleanup path called `coordination skip
<claimed_issue>` whenever `claimed_issue > 0 and pr_number == 0`, with
no `worker_type` guard. For repair/replan agents (which never own a
claimed issue), this called `skip` on whatever stale number the
parser had written, repeatedly failed (closed issue), and pod
re-dispatched the agent — a tight burn loop.

Fixes:
- Anchor the claim regex to start-of-line with MULTILINE, AND skip the
  detection entirely for `repair`/`replan` workers.
- Guard the cleanup-skip and quota-release paths with the same
  worker_type check.
"""
from __future__ import annotations

import json
import unittest
from unittest import mock

from pod import cli


def _agent_state(worker_type: str, **kw) -> cli.AgentState:
    s = cli.AgentState()
    s.short_id = "test"
    s.uuid = "00000000-0000-0000-0000-000000000000"
    s.worker_type = worker_type
    for k, v in kw.items():
        setattr(s, k, v)
    return s


def _codex_command_event(cmd: str, output: str) -> bytes:
    return json.dumps({
        "type": "item.completed",
        "item": {
            "type": "command_execution",
            "command": cmd,
            "aggregated_output": output,
        },
    }).encode()


def _claude_tool_result(text: str) -> bytes:
    return json.dumps({
        "type": "user",
        "message": {
            "content": [
                {"type": "tool_result", "content": text},
            ],
        },
    }).encode()


class CodexParserClaimedIssueTests(unittest.TestCase):
    """The codex parser's claimed-issue regex must:
      (a) only set state.claimed_issue from a canonical, line-anchored
          `Claimed issue #N` line (the output of `coordination claim`);
      (b) never set it for `repair` or `replan` workers.
    """

    def test_work_agent_canonical_claim_sets_claimed_issue(self):
        state = _agent_state("work")
        cli._parse_codex_jsonl_line(
            _codex_command_event("coordination claim 42",
                                 "Claimed issue #42\n"),
            state)
        self.assertEqual(state.claimed_issue, 42)

    def test_work_agent_ignores_mid_line_historical_mention(self):
        # `git log` or `coordination orient` may surface old claim
        # comments. The anchored regex must not match them.
        state = _agent_state("work")
        cli._parse_codex_jsonl_line(
            _codex_command_event(
                "git log --oneline -20",
                "abc123 chore: note that we Claimed issue #3399 last week\n"),
            state)
        self.assertEqual(state.claimed_issue, 0)

    def test_repair_agent_never_sets_claimed_issue(self):
        state = _agent_state("repair")
        # Even a perfectly-formed line is ignored.
        cli._parse_codex_jsonl_line(
            _codex_command_event("coordination orient",
                                 "Claimed issue #3399\n"),
            state)
        self.assertEqual(state.claimed_issue, 0)

    def test_replan_agent_never_sets_claimed_issue(self):
        state = _agent_state("replan")
        cli._parse_codex_jsonl_line(
            _codex_command_event("coordination list-replan",
                                 "Claimed issue #3399\n"),
            state)
        self.assertEqual(state.claimed_issue, 0)

    def test_repair_agent_still_records_repair_pr(self):
        # The repair_pr regex must continue to fire for repair workers.
        state = _agent_state("repair")
        cli._parse_codex_jsonl_line(
            _codex_command_event("coordination claim-pr-repair 3453",
                                 "Claimed PR #3453 for repair\n"),
            state)
        self.assertEqual(state.repair_pr, 3453)
        self.assertEqual(state.claimed_issue, 0)


class ClaudeParserClaimedIssueTests(unittest.TestCase):
    """Same invariants for the Claude JSONL parser."""

    def test_work_agent_canonical_claim_sets_claimed_issue(self):
        state = _agent_state("work")
        cli._parse_claude_jsonl_line(
            _claude_tool_result("Claimed issue #42\n"), state)
        self.assertEqual(state.claimed_issue, 42)

    def test_work_agent_ignores_mid_line_historical_mention(self):
        state = _agent_state("work")
        cli._parse_claude_jsonl_line(
            _claude_tool_result(
                "Earlier we Claimed issue #3399 but moved on.\n"),
            state)
        self.assertEqual(state.claimed_issue, 0)

    def test_repair_agent_never_sets_claimed_issue(self):
        state = _agent_state("repair")
        cli._parse_claude_jsonl_line(
            _claude_tool_result("Claimed issue #3399\n"), state)
        self.assertEqual(state.claimed_issue, 0)

    def test_replan_agent_never_sets_claimed_issue(self):
        state = _agent_state("replan")
        cli._parse_claude_jsonl_line(
            _claude_tool_result("Claimed issue #3399\n"), state)
        self.assertEqual(state.claimed_issue, 0)

    def test_repair_agent_still_records_repair_pr(self):
        state = _agent_state("repair")
        cli._parse_claude_jsonl_line(
            _claude_tool_result("Claimed PR #3453 for repair\n"), state)
        self.assertEqual(state.repair_pr, 3453)
        self.assertEqual(state.claimed_issue, 0)


class CleanupSkipPathGuardTests(unittest.TestCase):
    """The end-of-session cleanup-skip path (cli.py around L6312)
    must not call `coordination skip` for `repair` or `replan`
    workers, regardless of what `claimed_issue` happens to hold.

    Rather than spin up the whole session-loop, we test the structural
    invariant via a string-level inspection: every `coordination skip`
    of `state.claimed_issue` is guarded by a worker-type check.
    """

    def test_cleanup_skip_callsite_is_worker_type_guarded(self):
        import inspect
        src = inspect.getsource(cli)
        # Find all guards on `state.claimed_issue > 0 and state.pr_number == 0`
        # — there are two: the quota-exhaustion release path and the
        # general cleanup-skip path. Both must exclude repair/replan.
        # Crude but stable check: the line containing the conjunction
        # must also reference `worker_type` within the same `if`.
        import re
        # Find every occurrence of the conjunction
        pattern = re.compile(
            r"state\.claimed_issue\s*>\s*0\s+and\s+state\.pr_number\s*==\s*0"
            r"(?:[\s\S]{0,200}?)(?:repair|replan|worker_type)",
        )
        matches = pattern.findall(src)
        # Every callsite must be near a worker_type/repair/replan reference.
        # If a new unguarded callsite is added, this fails.
        callsite_count = len(re.findall(
            r"state\.claimed_issue\s*>\s*0\s+and\s+state\.pr_number\s*==\s*0",
            src))
        self.assertEqual(
            len(matches), callsite_count,
            f"Found {callsite_count} `claimed_issue > 0 and pr_number == 0` "
            f"call-sites but only {len(matches)} guarded by repair/replan/"
            f"worker_type — every such call-site must skip repair/replan "
            f"workers, otherwise pod will spin on closed issues.",
        )


if __name__ == "__main__":
    unittest.main()
