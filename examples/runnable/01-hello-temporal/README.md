# Hello Temporal: runnable lab (Java · Python · Go)

The smallest end-to-end Temporal program in three SDKs: a Workflow that calls one
Activity and returns its greeting.

The Worker and the client (starter) are **separate, standalone processes**, as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name (`hello-temporal`) and the
Workflow definition. Run the Worker in one terminal and the starter in another.
Order doesn't matter: start the Workflow first and the server holds it on the
queue until a Worker polls.

Start a dev server first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh hello java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh hello java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../hello/HelloWorker.java` (Worker) and
`HelloStarter.java` (client). Workflow + Activities are the shared
`GreetingWorkflowImpl` / `GreetingActivitiesImpl`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh hello python worker
#   scripts/run-example.sh hello python starter
```

Entry points: `python/worker.py` and `python/starter.py` (workflow + activity
in `python/greeting.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh hello go worker
#   scripts/run-example.sh hello go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow and
Activity live in `go/greeting.go` (package `hello`) so both commands import
them.

## Expected output

The **starter** prints:

```
Hello, Ada from a Temporal Activity
```

Then `scripts/show-example.sh ../../01-foundations/history_cli.sh` and run
`temporal workflow show --workflow-id hello-temporal-demo` to read the history.
