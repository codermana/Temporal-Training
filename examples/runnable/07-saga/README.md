# Order saga — runnable lab (Java · Python · Go)

The same order **saga** in three SDKs: `authorize payment → reserve inventory →
ship`, with a **compensation** for each forward step. If `ship` fails, the
compensations unwind in reverse (`restore inventory`, then `cancel payment`) so a
customer is never charged for goods they won't receive.

The teaching point is identical everywhere: **register a compensation only after
its forward step succeeds, and run them in reverse on failure.** Java has a
built-in `Saga` helper; Python and Go don't, so they manage the compensation stack
by hand — a list/slice you unwind in reverse.

> **Bound the retries.** Each forward Activity uses `maximumAttempts(3)`. Without
> it, a permanent `ship` failure retries forever and the saga never reaches
> compensation — the #1 saga mistake.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

Each Worker is long-lived: it polls the `orders` queue and you start Workflows
from the CLI.

```bash
# Happy path -> COMPLETED
temporal workflow start --task-queue orders --type OrderSagaWorkflow \
  --workflow-id order-OK --input '"order-1001"'

# Failing path -> COMPENSATED (ship throws, compensations run)
temporal workflow start --task-queue orders --type OrderSagaWorkflow \
  --workflow-id order-fail --input '"fail-at-ship"'
```

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java        # or: make run-saga
```

Entry point: `java/src/main/java/training/temporal/saga/SagaWorker.java`.

## Python (`temporalio`)

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python worker.py
```

Entry point: `python/worker.py` (workflow + activities in `python/saga.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (workflow + activities in `go/saga.go`).

## Expected output

`order-OK` returns `COMPLETED`; `order-fail` returns `COMPENSATED`. In the Web UI
history of the failing run you'll see `cancelPayment` and `restoreInventory`
Activities executing **after** `ship` exhausts its retries — proof the saga
unwound in reverse.

```bash
temporal workflow show --workflow-id order-fail --output json \
  | jq -r '.events[] | select(.eventType|test("ActivityTask")) | .eventType'
```
