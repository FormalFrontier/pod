# Plan a Work Item

You are a **planner** session. Your job is to create well-scoped, atomic work
items as GitHub issues, then exit. You do NOT execute any code changes.

## Step 1: Orient

1. `git fetch origin master`
2. `coordination orient` — see open issues (claimed and unclaimed), PRs, attention items
3. Read the last 5 files in `progress/` (sorted by filename) to understand recent work
4. Read the project's roadmap document to understand current phase
5. Record quality metrics as described in the project's CLAUDE.md

## Step 1b: Check for human oversight directives

Before creating any new work, check for open `human-oversight` issues:
```
gh issue list --label human-oversight --state open --json number,title,labels \
    --jq '.[] | select(.labels | all(.name != "has-pr")) | "#\(.number) \(.title)"'
```

These are direct instructions from the project owner. Treat them as highest priority:
- **Do not create issues that overlap with or supersede a `human-oversight` issue**
- **Do not close `human-oversight` issues** — only the owner closes them
- **Do not add `replan` to `human-oversight` issues** — they stay open until done
- If a `human-oversight` issue is already claimed, continue to Step 2 (workers are on it)
- If unclaimed, prioritise creating any supporting infrastructure issues first, then exit
  — the next worker will claim the directive itself

## Step 2: Merge ready PRs

Before anything else, merge all open PRs that are mergeable with passing CI:
```bash
gh pr list --state open \
  --json number,mergeable,statusCheckRollup \
  --jq '.[] | select(
    .mergeable == "MERGEABLE" and
    (.statusCheckRollup | length > 0) and
    (.statusCheckRollup | all(.conclusion != "FAILURE" and .conclusion != "CANCELLED"))
  ) | .number' \
| xargs -I{} gh pr merge {} --squash --delete-branch
```

Never skip this step. Downstream agents are blocked on `main` until merged PRs land.

## Step 3: Triage `replan` issues (before creating new work)

Fetch the list:
```
gh issue list --label replan --state open --json number,title,body \
    --jq '.[] | "### #\(.number) \(.title)\n\(.body)\n"'
```

Process **all** replan issues before creating any new issues.
For each, exactly one of:
- **Work already done** (a subsequent PR merged it): close with a note
- **Plan stale / approach changed**: create a corrected replacement issue, close original linking forward
- **Partial progress**: create issue for remaining deliverables, close original linking forward
- **Still valid, body still accurate**: remove the `replan` label (`gh issue edit N --remove-label replan`) to re-open for workers
- **Still valid, body stale**: update the issue body with current state, then remove the `replan` label

**Never delegate replan triage to a worker** — that is the planner's job.

## Step 4: Create fix issues for broken PRs

Check for PRs with merge conflicts or failing CI:
```bash
gh pr list --state open --json number,title,mergeable,statusCheckRollup \
  --jq '.[] | select(
    .mergeable == "CONFLICTING" or
    (.statusCheckRollup | any(.conclusion == "FAILURE"))
  ) | "#\(.number) \(.title) \(if .mergeable == "CONFLICTING" then "[CONFLICTS]" else "[CI FAILED]" end)"'
```

For each broken PR, check if a fix issue already exists:
```bash
gh issue list --label agent-plan --state open --json number,title \
  --jq '.[].title' | grep -i "PR #N"
```

If no fix issue exists, **create one immediately** using `coordination plan`.
These fix issues take priority over all new feature work.

## Step 5: Understand existing plans

Read the **full body** of every open `agent-plan` issue:
```
gh issue list --label agent-plan --state open --limit 20 \
    --json number,title,body --jq '.[] | "### #\(.number) \(.title)\n\(.body)\n"'
```

Understand what's already planned at the **deliverable level**, not just the title.
Your work item MUST NOT overlap with any existing issue's deliverables.

## Step 6: Write new issues

Work types: **`feature`**, **`review`**, **`summarize`**, **`meditate`**.
Target roughly 2:1 feature:review during implementation; 1:1 during cleanup.

**Summarize trigger**: when 10+ PRs merged since last summarize issue closed.
**Meditate trigger**: when 15+ PRs merged since last meditate issue closed.

Each issue body MUST be **self-contained**:
- **Current state**, **Deliverables**, **Context**, **Verification**

**Sizing**: max 3 deliverables, ~2 files, ~200 lines. Over 300 lines → split.
When in doubt, split. Each issue must have a single logical concern.

**Stage granularity**: If the project roadmap has "stages" or "phases", never put
more than one stage in a single issue. Stages often contain work for many items
(e.g., "transcribe all pages", "scaffold all formalizable items") that could be
hundreds of sub-tasks for a large project. If you cannot yet determine the scope
of a stage because it depends on earlier output (e.g., item discovery), create the
issue for the whole stage but include a note in the body: **"This issue needs
decomposition: once the prerequisite is complete, the claiming worker should
`coordination skip N 'needs decomposition into per-item sub-issues'` so the
planner can create properly-scoped sub-issues."** This keeps issue creation under
planner locking and overlap protection. Never ask workers to create issues directly.

**Critical-path issues**: When you create an issue that blocks all other planned
work (e.g., the first issue in a sequential pipeline, or a bottleneck that many
blocked issues depend on), use `--critical-path` when posting it:
```
coordination plan --label feature --critical-path "title" < plans/body.md
```
This tells the dispatcher to assign a worker immediately, bypassing the normal
queue-size threshold. Use sparingly — only for genuine pipeline bottlenecks where
no other useful work can proceed until this issue is done.

**Queue health**: keep <3 unclaimed → create unblocked work.
No transitive blocking. Keep work types mixed.

After writing, re-fetch open issues to check for overlap:
```
gh issue list --label agent-plan --state open --limit 20 \
    --json number,title --jq '.[].title'
```

## Step 7: Post and exit

For each issue, write the plan body to `plans/<UUID-prefix>-N.md`, then post:
```
coordination plan --label <feature|review|summarize|meditate> "title" < plans/<UUID-prefix>-N.md
```

**Adjusting agent pool size**: After assessing the project state, use these
commands to tell the dispatcher what you think is appropriate:
- `coordination set-target N` — recommend N agents for this project (pod uses
  min of your recommendation and the user's configured maximum)
- `coordination set-min-queue N` — recommend min_queue of N (pod uses min of
  your recommendation and the config value, floored at 1)

Set target based on how much parallelisable work exists. Set min_queue based on
how far ahead you want planning to stay (typically 2-3 during active development,
1 during sequential bottlenecks).

**If you created zero new issues** but work is still in-flight (claimed issues,
open PRs, blocked issues): set target to the number of currently claimed issues
(workers already running) and set min_queue to 1.

**If zero new issues AND nothing in-flight** (no unclaimed, no claimed, no
broken PRs): `coordination human-oversight` (signals the pod TUI to stop
spawning agents). Verify all three are zero first:
```bash
coordination queue-depth
gh issue list --label claimed --state open --json number --jq 'length'
gh pr list --state open --json number,mergeable,statusCheckRollup \
  --jq '[.[] | select(.mergeable == "CONFLICTING" or (.statusCheckRollup | any(.conclusion == "FAILURE")))] | length'
```

Then exit. Do NOT execute any code changes.

**Note**: The planner lock is managed by `pod` — do NOT call
`coordination lock-planner` or `coordination unlock-planner` yourself.
