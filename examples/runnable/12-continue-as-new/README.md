# Continue-as-new — runnable lab (Java · Python · Go)

The same long-running counter in three SDKs. Each Workflow run processes a small
**batch** of work and then **continue-as-news**: it ends the current run and
starts a fresh one with the same Workflow ID, carrying forward only the state the
next run needs (`processedSoFar`). This keeps Event History small no matter how
long the job runs — the standard pattern for indefinitely-running or
high-iteration Workflows.

The teaching point is identical everywhere: **continue-as-new replaces the run; it
does not return.** Carry forward only what the next run needs.

The Worker and the client (starter) are **separate, standalone processes** — as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`continue-as-new`) and the
Workflow definition. Run the Worker in one terminal and the starter in another.
Order doesn't matter: start the Workflow first and the server holds it on the
queue until a Worker polls.

Connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

This demo processes `TOTAL = 9` items in batches of `3`, so it chains across
**three** runs before completing.

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh continue java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh continue java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../continueasnew/ContinueAsNewWorker.java` (Worker) and
`ContinueAsNewStarter.java` (client). The Workflow is the shared
`CounterWorkflowImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh continue python worker
#   scripts/run-example.sh continue python starter
```

Entry points: `python/worker.py` and `python/starter.py` (workflow in
`python/counter.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh continue go worker
#   scripts/run-example.sh continue go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow lives in
`go/counter.go` (package `continueasnew`) so both commands import it.

## Expected output

```
Result: completed after 9 iterations
```

The client's `getResult` / `run.Get` transparently follows the chain to the final
result. In the Web UI the single Workflow ID `continue-as-new-demo` shows multiple
**Runs** linked by `WorkflowExecutionContinuedAsNew` — each run's history stays
small.
