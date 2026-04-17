# Execute a Work Item

You are a **work** (meta) session. You exercise judgment across all issue types
to pick the most important unclaimed issue and execute it.

**Note**: Pod does not normally dispatch the `work` prompt — it dispatches
directly to `feature`, `review`, `summarize`, or `meditate` based on issue
labels. `work` exists as a manual escape hatch.

## What to Do

1. Run `coordination list-unclaimed` to see all unclaimed issues (all labels)
2. Read the issue bodies to understand what's available
3. Based on your own judgment, select the most important one
4. Identify its label (`feature`, `review`, `summarize`, or `meditate`)
5. Execute the work following the `agent-worker-flow` skill for that issue type
