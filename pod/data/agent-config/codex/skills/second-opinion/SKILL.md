---
name: second-opinion
description: Get a second opinion from Claude. Use when the user asks for a second opinion, wants to validate an approach, or says to check with Claude. Works for planning, code review, implementation decisions, bug diagnosis, or final review.
allowed-tools: Bash, Read, Grep, Glob
---

# Second Opinion from Claude

Get a second perspective from Claude on your current work. This is useful for plans, implementation choices, bug diagnosis, code review, and final sanity checks.

## Quick Usage

```bash
# Ask Claude for a second opinion with context via stdin
skills/second-opinion/codex-opinion "What issues might exist in this approach?"

# Pipe in a diff for review
git diff --staged | skills/second-opinion/codex-opinion "Review these changes for bugs or risks"

# Review one file
cat src/main.ts | skills/second-opinion/codex-opinion "What could be improved here?"
```

The wrapper script automatically:
- Runs Claude from the current working directory
- Allows read-oriented tools so Claude can inspect the codebase
- Passes along optional stdin context
- Points Claude at Codex history in `~/.codex/history.jsonl`

## When To Use

Use this skill when you want an external check on:

- Architecture or planning decisions
- Mid-implementation tradeoffs
- Bug diagnosis and failure modes
- Code review for correctness, maintainability, or edge cases
- Final review before handing results back to the user

## Prompting Guidance

Provide:

1. A short summary of the current situation
2. The exact question you want Claude to answer
3. Relevant code, logs, or diffs via stdin when helpful
4. Constraints and what has already been considered

Prefer prompts that invite criticism rather than agreement. Examples:

- `What would you challenge in this approach?`
- `Review this diff for bugs, regressions, and missing tests`
- `What edge cases am I likely missing?`
- `What would a skeptical senior engineer flag here?`

## Interpreting Results

Treat Claude's response as an independent perspective. Compare it with your own analysis, investigate disagreements, and synthesize the useful parts into the final answer for the user.
