# Hello Temporal, anywhere: runnable lab (Java · Python · Go)

The same Hello workflow, but the **connection is env-driven** so one binary
targets a local dev server (Lab 1.2b Docker) or Temporal Cloud (Lab 1.2c) with no
code change. The connection mode is chosen by which variables are set:

| Variables set | Mode |
| --- | --- |
| _none_ | Plaintext to `TEMPORAL_ADDRESS` (default `127.0.0.1:7233`), local / Docker |
| `TEMPORAL_API_KEY` | Temporal Cloud over TLS via API key |
| `TEMPORAL_TLS_CERT` + `TEMPORAL_TLS_KEY` | Temporal Cloud over mTLS |

Shared: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE` (default `default`).

The Worker and the client (starter) are **separate, standalone processes**, as
they are in production. They never talk to each other directly; both only talk
to the Temporal server, agreeing on a Task Queue name and the Workflow
definition. Run the Worker in one terminal and the starter in another. Order
doesn't matter: start the Workflow first and the server holds it on the queue
until a Worker polls. The **same env-driven connection applies to both**: set
the same `TEMPORAL_*` variables for the Worker and the starter so they dial the
same server. The starter additionally reads `GREET_NAME` (default `Ada`) to
choose the greeting input.

## Java (`io.temporal:temporal-sdk`)

```bash
cd java
mvn -q compile exec:java                                       # terminal 1: Worker (default mainClass)
mvn -q compile exec:java -Dexec.mainClass=training.temporal.hello.HelloStarter   # terminal 2: starts one Workflow
```

Entry points: `java/.../hello/HelloWorker.java` (Worker) and `HelloStarter.java`
(client). Workflow + Activities are the shared `GreetingWorkflowImpl` /
`GreetingActivitiesImpl`; the connection is built by `Connections.fromEnv()`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
```

Entry points: `python/worker.py` and `python/starter.py` (workflow + activity in
`python/greeting.py`, connection helper in `python/connections.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
```

Entry points: `go/worker/main.go` and `go/starter/main.go`. The Workflow and
Activity live in `go/greeting.go` (package `hello`) and the connection helper in
`go/connections.go`, so both commands import them.

Cloud example (set the same env on **both** the Worker and the starter, in their
respective terminals).

```bash
# terminal 1: Worker
TEMPORAL_ADDRESS=us-east-1.aws.api.temporal.io:7233 \
TEMPORAL_NAMESPACE=your-ns.acct \
TEMPORAL_API_KEY=$(cat key.txt) \
  uv run worker.py

# terminal 2: starter
TEMPORAL_ADDRESS=us-east-1.aws.api.temporal.io:7233 \
TEMPORAL_NAMESPACE=your-ns.acct \
TEMPORAL_API_KEY=$(cat key.txt) \
GREET_NAME=Grace \
  uv run starter.py
```

The connection helpers (`Connections.java`, `connections.py`, `connections.go`)
are the only thing that differs from `01-hello-temporal`.
