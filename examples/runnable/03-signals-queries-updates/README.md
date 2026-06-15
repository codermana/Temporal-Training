# Signals, Queries & Updates — runnable lab (Java · Python · Go)

The same approval Workflow in three SDKs. The Workflow blocks waiting for a
decision, so while it is alive you can:

- **Query** its current state (read-only, no history event),
- **Update** the attached note (synchronous, *validated*, returns a result),
- **Signal** an approve/reject decision (fire-and-forget, unblocks the Workflow).

Each entry point starts a long-lived Worker plus one waiting `approval-demo`
Workflow, then stays running so you can drive it from the CLI.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/approval/ApprovalWorker.java`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py
```

Entry point: `python/worker.py` (Workflow in `python/approval.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (Workflow in `go/approval.go`).

## Drive it from another terminal

Names differ per SDK — handlers are named after the method (Java/Go: `currentState`,
`changeNote`; Python: `current_state`, `change_note`).

```bash
# Query (Java / Go handler names shown; Python uses snake_case)
temporal workflow query  --workflow-id approval-demo --type currentState

# Update — runs the validator first; an empty note is rejected
temporal workflow update execute --workflow-id approval-demo --name changeNote \
    --input '"expedite before close of business"'

# Signal — unblocks the Workflow, which then completes
temporal workflow signal --workflow-id approval-demo --name approve \
    --input '"manager@example.com"'
```

## Expected output

After the approve Signal the Workflow returns, e.g.:

```
PO-1001 APPROVED by manager@example.com (expedite before close of business)
```

The Query produced no history event; the Update added `WorkflowExecutionUpdateAccepted`
+ `...Completed`; the Signal added `WorkflowExecutionSignaled`.
