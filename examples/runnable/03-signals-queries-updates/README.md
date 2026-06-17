# Signals, Queries & Updates: runnable lab (Java · Python · Go)

The same approval Workflow in three SDKs. The Workflow blocks waiting for a
decision, so while it is alive you can:

- **Query** its current state (read-only, no history event),
- **Update** the attached note (synchronous, *validated*, returns a result),
- **Signal** an approve/reject decision (fire-and-forget, unblocks the Workflow).

The Worker and the client (starter) are **separate, standalone processes**, as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`approval`) and the
Workflow definition. Run the Worker in one terminal and the starter in another.
The starter kicks off one waiting `approval-demo` Workflow; the Worker keeps
polling so the Workflow stays alive and queryable while you drive it from the CLI.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh approval java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh approval java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../approval/ApprovalWorker.java` (Worker) and
`ApprovalStarter.java` (client). Workflow lives in `ApprovalWorkflowImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh approval python worker
#   scripts/run-example.sh approval python starter
```

Entry points: `python/worker.py` and `python/starter.py` (Workflow in
`python/approval.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh approval go worker
#   scripts/run-example.sh approval go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow lives
in `go/approval.go` (package `approval`) so both commands import it.

## Drive it from another terminal

Names differ per SDK; handlers are named after the method (Java/Go: `currentState`,
`changeNote`; Python: `current_state`, `change_note`).

```bash
# Query (Java / Go handler names shown; Python uses snake_case)
temporal workflow query  --workflow-id approval-demo --type currentState

# Update: runs the validator first; an empty note is rejected
temporal workflow update execute --workflow-id approval-demo --name changeNote \
    --input '"expedite before close of business"'

# Signal: unblocks the Workflow, which then completes
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
