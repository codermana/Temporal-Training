# Child Workflows — runnable lab (Java · Python · Go)

The same parent/child fan-out in three SDKs: a `BatchWorkflow` spawns one
`ItemWorkflow` **per item**, in parallel, then waits for all of them. The
teaching point is that each child gets its own **stable Workflow ID**
(`item-A`, `item-B`, `item-C`) — so it has its own history and is separately
queryable, signalable, and cancelable, unlike an Activity.

The Worker and the client (starter) are **separate, standalone processes** — as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`child-workflows`) and the
Workflow definitions. The Worker registers **both** the parent (`BatchWorkflow`)
and the child (`ItemWorkflow`); the starter only starts the parent. Run the
Worker in one terminal and the starter in another. Order doesn't matter: start
the Workflow first and the server holds it on the queue until a Worker polls.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh child java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh child java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../child/ChildWorker.java` (Worker) and
`ChildStarter.java` (client). Workflow impls are the shared
`BatchWorkflowImpl` / `ItemWorkflowImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh child python worker
#   scripts/run-example.sh child python starter
```

Entry points: `python/worker.py` and `python/starter.py` (Workflows in
`python/batch.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh child go worker
#   scripts/run-example.sh child go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflows live
in `go/batch.go` (package `child`) so both commands import them.

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
