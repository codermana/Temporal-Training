# Async & parallel Activities — runnable lab (Java · Python · Go)

The same order-pricing Workflow in three SDKs: price every SKU **in parallel**,
then sum. The teaching point is identical everywhere — *start every Activity
before you wait on any of them.*

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh async          # == cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/parallel/PricingWorker.java`.

## Python (`temporalio`)

```bash
cd python
uv run worker.py
# or, from the repo root:  scripts/run-example.sh async python
```

Entry point: `python/worker.py` (workflow + activities in `python/pricing.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
# or, from the repo root:  scripts/run-example.sh async go
```

Entry point: `go/main.go` (workflow + activity in `go/pricing.go`).

## Expected output

```
Total price: 355      # book(30) + lamp(75) + desk(250)
```

In the Web UI history all `ActivityTaskScheduled` events are emitted in the
**same** Workflow Task — proof the fan-out ran concurrently.
