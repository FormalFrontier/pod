# pod

Multi-agent manager for Claude Code and Codex. Launches and monitors concurrent
autonomous agent sessions, coordinating via GitHub issues, labels, and PRs.

## Quick Start

```bash
# Install
uv tool install git+https://github.com/FormalFrontier/pod.git

# For development: uv pip install -e .

# Bootstrap a project
cd your-project
pod init

# Now write your long-term PLAN.md

# Launch the TUI
pod

# Or use CLI commands
pod add 3        # launch 3 agents
pod status       # queue depth, agent count, cost
pod list         # show running agents
```

## How It Works

Pod manages a pool of autonomous agents, each running in its
own git worktree on its own branch. Agents coordinate through GitHub
issues and PRs:

1. Write a `PLAN.md` describing the project goals, stages, and structure
2. **Planners** read `PLAN.md` and create scoped work items as GitHub issues
3. **Workers** claim issues, implement changes, and open PRs
4. Auto-merge handles the rest

`PLAN.md` is the single source of truth — planners decompose it into
issues, workers execute those issues.  Agents never modify `PLAN.md`
itself; update it directly when you want to steer the project.

## Requirements

- Python 3.10+
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) (`claude`) or OpenAI Codex (`codex`)
- [GitHub CLI](https://cli.github.com/) (`gh`), authenticated
- Git

## Commands

| Command | Description |
|---------|-------------|
| `pod` | Interactive TUI |
| `pod init [--force]` | Bootstrap `.pod/` in current git repo |
| `pod update` | Re-populate agent config from installed package |
| `pod add [N]` | Launch N agents (default 1) |
| `pod list` | Show running agents |
| `pod finish [ID\|all]` | Signal agent(s) to finish after current work |
| `pod kill [ID\|all]` | Kill agent(s) immediately |
| `pod status` | Queue depth, agent count, total cost |
| `pod log [ID]` | Tail agent's session output |
| `pod config [--edit]` | Show or edit configuration |
| `pod coordination ...` | Run bundled coordination script directly |

## Configuration

After `pod init`, edit `.pod/config.toml` to customize:

- **Worker types**: define agent roles (`/plan`, `/feature`, `/review`, etc.)
- **Dispatch strategy**: `queue_balance` or `round_robin`
- **Claude model**: default `opus`
- **Build cache**: directory to rsync into worktrees
- **Protected files**: files agents may not modify in PRs

For Claude, agent session config (commands, skills) lives in
`.pod/claude-config/` and is managed by pod -- run `pod update` after
upgrading pod to get the latest agent prompts.

For Codex, `isolated_config = true` creates a strict pod-managed
`CODEX_HOME` per worktree. It contains only pod-managed Codex skills, a
minimal pod-owned `config.toml`, and an `auth.json` symlink for login
reuse. Pod does not inherit global `~/.codex` prompts, skills, plugins,
or session state into isolated homes. This intentionally drops user
Codex configuration such as custom providers, base URLs, MCP settings,
and other global config; isolated Codex sessions use only pod-managed
configuration plus the shared auth token.

### Public-repo safety: GitHub interaction limits

Pod feeds issue bodies and comments on labeled issues into agent
prompts. On a public repo with no interaction limit, anyone with a
GitHub account can post text into that prompt stream. Before
dispatching agents, pod checks
`gh api repos/<owner>/<repo>/interaction-limits` and refuses to run if:

- the repo is public and has no interaction limit set;
- the limit is weaker than the configured minimum (default
  `collaborators_only`); or
- the limit expires within `minimum_expiry_days` (default 7).

Set or renew the limit with:

```sh
gh api -X PUT repos/<owner>/<repo>/interaction-limits \
  -f limit=collaborators_only -f expiry=six_months
```

The check runs at the top of `spawn_agent`, so every dispatch path is
gated (`pod add`, TUI auto-spawn, dead-session restart). Results are
cached for 5 minutes to bound API calls during burst spawning while
still detecting limit removal/expiry inside long-running pod processes.

GitHub interaction limits expire (max `six_months`), so you'll need to
renew periodically. The `[security]` section of `.pod/config.toml`
exposes `enforce_interaction_limits`, `minimum_interaction_limit`, and
`minimum_expiry_days` if you need a different policy.

**What this does *not* protect against:** interaction limits are
forward-only. They block who can post *new* issues and comments, but
they don't reach back through history. An issue body authored before
the limit was enabled, or comments left on an issue *before* a
collaborator applied the `agent-plan` / `directive` label, are
still ingested verbatim by the interaction-limit check alone. The
per-message provenance gate below covers that residual gap.

### Per-message provenance gate

A second layer that closes the historical-content gap. Pod refuses to
surface issue bodies / comments authored by accounts without a
trusted association with the repo. Decided per message using GitHub's
`authorAssociation` field on every issue and comment, so it works
without admin scope and covers content posted before any
interaction-limit was set.

Trusted by default: `OWNER`, `MEMBER`, `COLLABORATOR`. Configurable
under `[security]`:

```toml
trust_only_collaborators = true
trusted_author_associations = ["OWNER", "MEMBER", "COLLABORATOR"]
trusted_users = []                # bot allowlist (e.g. "dependabot[bot]")
provenance_cache_seconds = 60
```

Behaviour on rejection:

- `coordination list-unclaimed` and `coordination orient` silently
  omit untrusted issues. Set `POD_INCLUDE_UNTRUSTED=1` to surface
  them annotated `[UNTRUSTED: <reason>]` for human triage.
- `coordination claim N` refuses with `CLAIM FAILED: ...`; the agent
  picks a different issue per the existing flow.
- `coordination read-issue N` refuses; this is the gate that the
  worker flow uses in place of `gh issue view N --json body`.
- The planner's pre-claim body reads (in `plan.md` Steps 3 and 5) go
  through `pod _filter-trusted-issues`, which drops untrusted issues
  before any body text reaches the agent.

Caching: list-time checks use a 60-second TTL; `claim` and
`read-issue` always re-check fresh, so a comment added between
discovery and read can't sneak past.

**Org-membership caveat:** `MEMBER` is org membership, not write
access on the specific repo. On an org repo it trusts every org
member, including users who can't push to this repo. For stricter
behaviour, drop `MEMBER` from `trusted_author_associations`.

**Bot allowlist:** service accounts (`dependabot[bot]`,
`github-actions[bot]`, custom CI/status bots that comment on issues)
typically have `authorAssociation == NONE`. Add them to
`trusted_users` so their comments don't trip the gate.

**What this layer does *not* cover** (named honestly so we don't
oversell):

- PR diffs and PR review comments — the `/repair` flow operates on
  PR metadata, but PR text is not run through this gate.
- CI logs — relevant to `/repair`, which can ingest log text.
- Repository file contents — if a malicious commit lands in a
  file-path an agent reads, this gate doesn't help.
- Linked issues/PRs that the agent fetches mid-session of its own
  initiative.
- Text a *trusted* collaborator copies into an issue from an
  untrusted source. Provenance ≠ semantic sanitization.

## Coordination

`pod coordination <subcommand>` runs the bundled coordination script
that agents use to interact with the GitHub issue queue. You can call
it directly for debugging or manual intervention.

### Issue lifecycle

| Subcommand | Description |
|------------|-------------|
| `orient` | Show current state: oversight directives, unclaimed issues, claimed issues, open PRs |
| `plan --label <type> [--critical-path] "title"` | Create an issue from stdin body with `agent-plan` + type label |
| `list-unclaimed [--label <type>]` | List unclaimed issues, optionally filtered by label |
| `queue-depth [<label>]` | Print count of unclaimed issues |
| `critical-path-depth` | Print count of unclaimed `critical-path` issues |
| `claim <N>` | Claim issue #N for the current session (label + race detection) |
| `skip <N> "reason"` | Release claim on #N and mark it `replan` |
| `add-dep <N> <M>` | Add `depends-on: #M` to issue #N; mark blocked if #M is open |
| `check-blocked` | Unblock issues whose dependencies are all closed |
| `release-stale-claims [seconds]` | Release claims older than threshold (default 4h) |
| `release-orphan-claims` | Release claims whose owning session UUID is not in `.pod/agents/` (no age threshold) |

### PRs

| Subcommand | Description |
|------------|-------------|
| `create-pr <N> [--partial] ["title"]` | Open a PR for issue #N (enforces protected files) |
| `claim-fix <N>` | Claim a broken PR for fixing (advisory, not strict lock) |
| `close-pr <N> "reason"` | Close PR #N with a reason |

### Planner lock

| Subcommand | Description |
|------------|-------------|
| `lock-planner` | Acquire the planner lock (managed by pod, not agents) |
| `unlock-planner` | Release the planner lock |
| `lock-status` | Show who holds the planner lock |
| `force-unlock-planner` | Force-release a stuck lock |

### Pool control (called by planners)

| Subcommand | Description |
|------------|-------------|
| `set-target <N>` | Recommend N agents; pod uses min(user target, planner target) |
| `set-min-queue <N>` | Recommend min_queue of N; pod floors at 1 |
| `return-to-human` | Signal pod to stop spawning and return control to operator |
| `check-return-to-human` | Check if the return-to-human signal is set |
| `clear-return-to-human` | Clear the signal to resume operation |

## License

Apache 2.0
