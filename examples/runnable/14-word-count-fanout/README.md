# Word count fan-out: runnable lab (Java · Python · Go)

Count words across document chunks in parallel, then sum the chunk totals. The
teaching point is the same as the async pricing lab: *start every Activity before
you wait on any of them.*

The Worker and the client (starter) are separate, standalone processes. Run the
Worker in one terminal and the starter in another. Both connect to a local
Temporal dev server.

Start Temporal first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh wordcount java worker     # terminal 1
scripts/run-example.sh wordcount java starter    # terminal 2
```

Entry points: `java/.../wordcount/WordCountWorker.java` and
`WordCountStarter.java`.

## Python (`temporalio`)

```bash
scripts/run-example.sh wordcount python worker   # terminal 1
scripts/run-example.sh wordcount python starter  # terminal 2
```

Entry points: `python/worker.py` and `python/starter.py`.

## Go (`go.temporal.io/sdk`)

```bash
scripts/run-example.sh wordcount go worker       # terminal 1
scripts/run-example.sh wordcount go starter      # terminal 2
```

Entry points: `go/worker/main.go` and `go/starter/main.go`.

## Expected output

The starter prints:

```text
Total words: 34
```

In the Web UI history, the `ActivityTaskScheduled` events for the chunks appear
in the same Workflow Task, showing that the fan-out ran concurrently.
