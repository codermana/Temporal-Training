# Child Workflows — runnable lab (Java · Python · Go)

The same parent/child fan-out in three SDKs: a `BatchWorkflow` spawns one
`ItemWorkflow` **per item**, in parallel, then waits for all of them. The
teaching point is that each child gets its own **stable Workflow ID**
(`item-A`, `item-B`, `item-C`) — so it has its own history and is separately
queryable, signalable, and cancelable, unlike an Activity.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/child/ChildWorker.java`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py
```

Entry point: `python/worker.py` (Workflows in `python/batch.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (Workflows in `go/batch.go`).

## Expected output

```
Parent result:
processed[A] in child item-A
processed[B] in child item-B
processed[C] in child item-C
```

In the Web UI you'll see one parent execution (`batch-parent-demo`) plus three
child executions `item-A` / `item-B` / `item-C`, each with its own history. The
`StartChildWorkflowExecutionInitiated` events are emitted in the same Workflow
Task — proof the children were started concurrently.
