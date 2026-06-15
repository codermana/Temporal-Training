# Retries & heartbeats — runnable lab (Java · Python · Go)

The same processing Workflow in three SDKs, built to make two reliability
mechanisms *visible* in history:

- **Retries** — `chargeCard` fails its first two attempts and succeeds on the
  third, so you see two `ActivityTaskFailed` events and the backoff between them.
- **Heartbeats** — `exportLargeReport` heartbeats once per page. The page number
  is the resume point: on a Worker restart the Activity continues from the last
  recorded page instead of starting over, and the heartbeat timeout is how a
  dead Worker is detected between pages.

The Worker and the client (starter) are **separate, standalone processes** — as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`retries-heartbeats`) and
the Workflow definition. Run the Worker in one terminal and the starter in
another. Order doesn't matter: start the Workflow first and the server holds it
on the queue until a Worker polls.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh retries java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh retries java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../retries/RetriesWorker.java` (Worker) and
`RetriesStarter.java` (client). Workflow + Activities are the shared
`ProcessingWorkflowImpl` / `FlakyActivitiesImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh retries python worker
#   scripts/run-example.sh retries python starter
```

Entry points: `python/worker.py` and `python/starter.py` (workflow + activities
in `python/processing.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh retries go worker
#   scripts/run-example.sh retries go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow and
Activity live in `go/processing.go` (package `retries`) so both commands import
them.

## Expected output

```
charge_card attempt 1 for order-42      # fails -> retried
charge_card attempt 2 for order-42      # fails -> retried
charge_card attempt 3 for order-42      # succeeds
exported page 1/5
...
exported page 5/5
Result: charged order-42 on attempt 3 | exported 5 pages
```

In the Web UI history you'll find two `ActivityTaskFailed` events before
`chargeCard` completes, and the recorded heartbeats on `exportLargeReport`.
