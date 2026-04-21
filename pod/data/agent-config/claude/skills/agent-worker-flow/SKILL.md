---
name: agent-worker-flow
description: Standard claim/branch/verify/publish workflow for pod agent sessions. Read this skill at the start of any feature, review, summarize, or meditate session.
allowed-tools: Bash, Read, Glob, Grep
---

# Standard Worker Flow for Pod Agent Sessions

This skill covers the shared workflow used by all pod worker agents.
Session-specific commands reference this skill rather than duplicating it.

## Coordination Reference

The `coordination` script handles all GitHub-based multi-agent coordination.
Session UUID is available as `$POD_SESSION_ID` (exported by `pod`).
The `gh` CLI defaults to the current repo, so `--repo` is not needed.

| Command | What it does |
|---------|-------------|
| `coordination orient` | List unclaimed/claimed issues, open PRs, PRs needing attention |
| `coordination plan [--label L] "title"` | Create GitHub issue with agent-plan + optional label; body from stdin |
| `coordination create-pr N [--partial] ["title"]` | Push branch, create PR closing issue #N, enable auto-merge, swap `claimed` → `has-pr`. With `--partial`: adds `replan` label. |
| `coordination claim-fix N` | Comment on failing PR #N claiming fix (30min cooldown) |
| `coordination close-pr N "reason"` | Comment reason and close PR #N |
| `coordination list-unclaimed [--label L]` | List unclaimed agent-plan issues (FIFO order); optional label filter |
| `coordination queue-depth [L]` | Count of unclaimed issues; optional label for per-type count |
| `coordination claim N` | Claim issue #N — adds `claimed` label + comment, detects races |
| `coordination skip N "reason"` | Mark claimed issue as needing replan — removes `claimed`, adds `replan` label |
| `coordination add-dep N M` | Add `depends-on: #M` to issue #N's body; adds `blocked` label if #M is open |
| `coordination check-blocked` | Unblock issues whose `depends-on` dependencies are all closed |
| `coordination release-stale-claims [SECS]` | Release claimed issues with no PR after SECS seconds (default 4h); **manual use only** |
| `coordination lock-planner` | Acquire advisory planner lock (20min TTL) |
| `coordination unlock-planner` | Release planner lock early |
| `coordination critical-path-depth [L]` | Count unclaimed critical-path issues; optional label filter |
| `coordination set-target N` | Planner sets recommended target agent count |
| `coordination set-min-queue N` | Planner sets recommended min_queue |

**Issue lifecycle**: planner creates issue (label: `agent-plan`) →
worker claims it (adds label: `claimed`) → worker creates PR closing it
(label swaps to `has-pr`) → auto-merge squash-merges.
Issues marked `replan` (by skip or partial completion) are handled by the next planner.
Issues with `has-pr` are excluded from `list-unclaimed` and `queue-depth`.

**Partial completion**: worker uses `--partial` → label swaps to
`replan`. A planner creates a new issue for remaining work, then closes
the `replan` issue with a link to the new one.

**Dependencies**: Issues can declare `depends-on: #N` in their body.
`coordination plan` auto-adds the `blocked` label if any dependency is
open. `check-blocked` (run by `pod` each loop) removes `blocked` when
all dependencies close. Blocked issues are excluded from
`list-unclaimed` and `queue-depth`.

**Branch naming**: `agent/<first-8-chars-of-UUID>`
**Plan files**: `plans/<UUID-prefix>.md`
**Progress files**: `progress/<UTC-timestamp>_<UUID-prefix>.md`

## Step 1: Claim a Work Item

```
coordination orient
```

**Priority order:**
0. **Human oversight directives first**: Check for open `human-oversight` issues before
   anything else. These are direct instructions from the project owner and take absolute
   precedence over all other work:
   ```
   coordination list-unclaimed --label human-oversight
   ```
   If any are open and unclaimed, claim the oldest one immediately.
   **These issues cannot be skipped or refused because you disagree with the approach.**
   The only valid exit from a `human-oversight` issue is completing it, or posting a
   comment explaining a genuine technical blocker (e.g. a missing dependency), then
   using `coordination skip` with that reason. Do not `skip` because you think a
   different approach is better — that is the owner's call, not yours.
1. **Oldest unclaimed issue** of your type:
   ```
   coordination list-unclaimed --label <your-label>
   ```

**Don't repair PRs from a worker session.** PR health (merge conflicts,
failed CI, stuck CI) is the `repair` agent's responsibility; pod dispatches
`/repair` automatically when `coordination list-pr-repair` reports
candidates, ahead of `/plan` / `/work`. Focus on fresh issue work.

If the queue is empty, write a brief progress note and exit.

```
coordination claim <issue-number>
```

**You MUST check the output.** If it says `CLAIM FAILED`, you MUST NOT work
on that issue — pick a different one. Only proceed if the output says
`Claimed issue #N`. Read the full issue body:
```
gh issue view <N> --json body --jq .body
```

## Step 2: Set Up

```bash
git checkout -b agent/<first-8-chars-of-session-UUID>
git rev-parse HEAD      # record starting commit
```

**If the branch already exists** (common in reused worktrees): check for an
open PR on it first (`gh pr list --head agent/<id>`). If a PR exists, create
a new branch with a suffix (`agent/<id>-v2`). If no PR exists, reset it to
master: `git checkout agent/<id> && git reset --hard origin/master`.

Record any project-specific quality metrics (e.g. sorry count, test coverage)
as described in the project's CLAUDE.md.

## Step 3: Codebase Orientation

Read the specific files mentioned in the plan/issue. Understand the current state
of code you'll be modifying. Don't read progress history — the issue body provides
that context.

## Step 4: Verify Assumptions

Check that the plan's assumptions still hold:
- Quality metrics match what the issue says
- Files mentioned in the issue still exist and haven't been restructured
- No recently merged PR invalidates the plan

If stale:
```
coordination skip <issue-number> "reason: <what changed>"
```
Go back to Step 1 and try the next issue.

**PR fix plans**: If the plan asks you to fix a broken PR, use judgement. If the
PR is low quality or not worth salvaging:
```
coordination close-pr <pr-number> "reason: <why not worth fixing>"
```

## Step 4b: Assess Scope

After orienting but **before writing code**, check whether the task fits
in a single session. Warning signs it doesn't:

- Target file is 500+ lines and you need to understand most of it
- The work naturally splits into independent sub-lemmas or sub-tasks
- Difficulty feels higher than the issue says

If the issue is too large, **decomposing it into smaller sub-issues is a
normal success path**, not a failure mode. You have the freshest codebase
context and can usually scope sub-tasks more accurately than a planner could
in advance. A good decomposition is more valuable than a failed heroic
attempt — and far better than overrunning the session trying to salvage it.

You may decompose when any of these is true:
- the claimed issue is too large for one session,
- the work naturally splits into independent sub-tasks,
- you can write self-contained successor issues without further investigation.

```bash
# Create self-contained sub-issues. Use `coordination plan` exactly as a
# planner would — same body template (Current state / Deliverables /
# Context / Verification), same label, same overlap-warning protection.
echo "body..." | coordination plan --label feature "Sub-task 1: ..."
echo "body..." | coordination plan --label feature "Sub-task 2: ..."

# Link ordering dependencies if any sub-task must precede another.
coordination add-dep <sub2> <sub1>
```

Then resolve the parent issue. Pick whichever fits:

- **Sub-issues fully cover the parent** (no residual scope): close the
  parent, linking forward so the planner doesn't re-triage it.
  ```bash
  gh issue close <parent> --comment "Decomposed into #X, #Y — superseded."
  gh issue edit <parent> --remove-label claimed
  ```
- **Parent still has residual scope or needs re-scoping**: skip it so the
  planner narrows the body to what's left.
  ```bash
  coordination skip <parent> "Decomposed into #X, #Y — narrow this to <residual>"
  ```

After decomposing, you have two options:

1. **Continue on one of the sub-issues**: claim it via `coordination claim`,
   then return to Step 2 with the sub-issue. Common case when the parent
   was just two work items glued together.
2. **Stop and exit**: if you've used most of your session orienting, write a
   brief progress entry and exit. The next worker will claim a sub-issue.

If you've already done a coherent subset of the parent's work *before*
deciding to decompose, prefer the partial-PR path instead: create sub-issues
for the remaining work, then `coordination create-pr <parent> --partial`.
That marks the parent `replan`; the next planner sees the sub-issues in the
issue's comments and closes the parent with a forward link.

## Step 5: Execute

After each coherent chunk of changes:
- Build and test using the project's build commands (see project CLAUDE.md)
- Commit with conventional prefixes: `feat:`, `fix:`, `refactor:`, `test:`, `doc:`, `chore:`

Each commit must compile. One logical change per commit.

**Commit early, create PRs early.** Sessions can terminate at any time.
Pushed-but-not-PR'd work is effectively lost — nobody will find it.

- Commit after every compiling milestone. Don't wait for the full feature.
- WIP commits are fine: `feat: WIP prove helper_lemma (2/4 sorries remain)`
- If 20+ minutes have passed without a commit, stop and commit now.
- Use `coordination create-pr N --partial` as soon as you have useful
  progress, even if incomplete. This saves the work as a visible PR.

**Failure handling:**
- Build fails on pre-existing issue → log and work around
- Stuck after 3 fundamentally different attempts → decompose into sub-issues (Step 4b)
- 3 consecutive iterations with no commits → end session, document blockers
  (does not apply to review or self-improvement sessions)
- If `/second-opinion` or `/reflect` is unavailable, skip and note in progress entry

## Step 5b: Context Health

**If conversation compaction occurs:**
1. Finish your current sub-task (get to compiling state)
2. Commit what you have
3. Skip remaining deliverables — do NOT start new work
4. Go directly to Step 6 then Step 7 with `--partial`

Commit early and often. Each commit is a checkpoint.

## Step 6: Verify

Build and test the project. Compare quality metrics with the starting values.
Review your diff: `git diff <starting-commit>..HEAD`.
Use `/second-opinion` if available.

## Step 7: Publish

Write a progress entry to `progress/<UTC-timestamp>_<UUID-prefix>.md`:
- Date/time (UTC), session type, what was accomplished
- Decisions made, key patterns discovered
- What remains, quality metric deltas

**Full completion:**
```bash
git push -u origin <branch>
coordination create-pr <issue-number>
```

**Once the PR is created, exit.** Do not poll CI, wait for the merge, or
otherwise spin on the PR. Another session will pick up any follow-up work
(e.g. a "fix PR #N" issue if CI fails). Polling burns context and tokens
for no benefit.

**Partial completion** (did NOT complete all deliverables):
- Progress entry lists: completed deliverables, NOT-completed deliverables and why,
  whether unfinished work needs a new issue
- Use `--partial`:
  ```
  coordination create-pr <N> --partial "feat: what was actually done"
  ```

**If you only closed a bad PR** (no code changes):
```bash
gh issue close <N> --comment "Closed PR #M as not worth salvaging. See progress entry."
```

## Step 8: Reflect

Run `/reflect`. If it suggests improvements to skills or commands, make those
changes and commit before finishing. Do NOT modify the project's top-level
CLAUDE.md or roadmap files — those are off-limits to agents.
