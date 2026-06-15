# Hello Temporal — runnable lab (Java · Python · Go)

The smallest end-to-end Temporal program in three SDKs: a Workflow that calls one
Activity and returns its greeting. Start a dev server first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh hello          # == cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/hello/HelloWorker.java`.

## Python (`temporalio`)

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python worker.py
# or, from the repo root:  scripts/run-example.sh hello python
```

Entry point: `python/worker.py` (workflow + activity in `python/greeting.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
# or, from the repo root:  scripts/run-example.sh hello go
```

Entry point: `go/main.go` (workflow + activity in `go/greeting.go`).

## Expected output

```
Hello, Ada from a Temporal Activity
```

Then `scripts/show-example.sh ../../01-foundations/history_cli.sh` and run
`temporal workflow show --workflow-id hello-temporal-demo` to read the history.
