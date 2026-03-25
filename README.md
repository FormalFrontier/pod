# pod

Multi-agent manager for Claude Code. Launches and monitors concurrent
autonomous Claude sessions, coordinating via GitHub issues, labels, and PRs.

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

Pod manages a pool of autonomous Claude Code agents, each running in its
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
- [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) (`claude`)
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

Agent session config (commands, skills) lives in `.pod/claude-config/`
and is managed by pod -- run `pod update` after upgrading pod to get
the latest agent prompts.

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
