# Continue-as-new — runnable lab (Java · Python · Go)

The same long-running counter in three SDKs. Each Workflow run processes a small
**batch** of work and then **continue-as-news**: it ends the current run and
starts a fresh one with the same Workflow ID, carrying forward only the state the
next run needs (`processedSoFar`). This keeps Event History small no matter how
long the job runs — the standard pattern for indefinitely-running or
high-iteration Workflows.

The teaching point is identical everywhere: **continue-as-new replaces the run; it
does not return.** Carry forward only what the next run needs.

Connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

This demo processes `TOTAL = 9` items in batches of `3`, so it chains across
**three** runs before completing.

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/continueasnew/ContinueAsNewWorker.java`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py
```

Entry point: `python/worker.py` (workflow in `python/counter.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (workflow in `go/counter.go`).

## Expected output

```
Result: completed after 9 iterations
```

The client's `getResult` / `run.Get` transparently follows the chain to the final
result. In the Web UI the single Workflow ID `continue-as-new-demo` shows multiple
**Runs** linked by `WorkflowExecutionContinuedAsNew` — each run's history stays
small.
