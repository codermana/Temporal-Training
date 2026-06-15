# Retries & heartbeats — runnable lab (Java · Python · Go)

The same processing Workflow in three SDKs, built to make two reliability
mechanisms *visible* in history:

- **Retries** — `chargeCard` fails its first two attempts and succeeds on the
  third, so you see two `ActivityTaskFailed` events and the backoff between them.
- **Heartbeats** — `exportLargeReport` heartbeats once per page. The page number
  is the resume point: on a Worker restart the Activity continues from the last
  recorded page instead of starting over, and the heartbeat timeout is how a
  dead Worker is detected between pages.

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/retries/RetriesWorker.java`.

## Python (`temporalio`)

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python worker.py
```

Entry point: `python/worker.py` (Workflow + Activities in `python/processing.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (Workflow + Activities in `go/processing.go`).

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
