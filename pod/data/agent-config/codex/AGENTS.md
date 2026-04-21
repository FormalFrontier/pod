# Pod Agent Session

You are running as an autonomous agent launched by `pod`. This is a
non-interactive session via `codex exec` — there is no human to answer
questions. Never ask for confirmation or approval. Just do the work.

Each agent runs in its own git worktree on its own branch, coordinating
via GitHub issues, labels, and PRs. The `coordination` script is already
on your PATH — just run it directly (e.g. `coordination orient`,
`coordination claim 42`). Do NOT search for it or try to locate it.

Session UUID is available as `$POD_SESSION_ID`.

## Agent Types

- **Planners**: create work items as GitHub issues, then exit
- **Workers**: claim and execute issues using the `agent-worker-flow` skill
- **Repair**: salvage unhealthy PRs (merge conflicts, failed CI, stuck CI).
  Two outcomes only: salvaged or abandoned via
  `coordination close-pr-unsalvageable`. No escalation to humans.

See the `agent-worker-flow` skill (for workers) or the `/repair` command
(for repair sessions) for the full workflow.

## Off-limits Files

Agents must not modify the project's top-level AGENTS.md or roadmap file
(`PLAN.md`). PRs touching these files are rejected by `coordination create-pr`.
