# Async & parallel Activities — runnable lab (Java · Python · Go)

The same order-pricing Workflow in three SDKs: price every SKU **in parallel**,
then sum. The teaching point is identical everywhere — *start every Activity
before you wait on any of them.*

The Worker and the client (starter) are **separate, standalone processes** — as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`pricing`) and the
Workflow definition. Run the Worker in one terminal and the starter in another.
Order doesn't matter: start the Workflow first and the server holds it on the
queue until a Worker polls.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh async java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh async java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../parallel/PricingWorker.java` (Worker) and
`PricingStarter.java` (client). Workflow + Activities are the shared
`OrderPricingWorkflowImpl` / `PricingActivitiesImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh async python worker
#   scripts/run-example.sh async python starter
```

Entry points: `python/worker.py` and `python/starter.py` (workflow + activities
in `python/pricing.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh async go worker
#   scripts/run-example.sh async go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow and
Activity live in `go/pricing.go` (package `parallel`) so both commands import
them.

## Expected output

The **starter** prints:

```
Total price: 355      # book(30) + lamp(75) + desk(250)
```

In the Web UI history all `ActivityTaskScheduled` events are emitted in the
**same** Workflow Task — proof the fan-out ran concurrently.
