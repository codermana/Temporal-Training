# Schedules: runnable lab (Java · Python · Go)

The same daily **Schedule** in three SDKs: fire `DailyReportWorkflow` at 09:00
every day, with `overlap=SKIP` (the Airflow `max_active_runs=1` equivalent).

Unlike the other labs, each entry point is a **one-shot client program**, not a
long-lived Worker: it registers the Schedule on the server and exits. The server
fires the Workflow on the spec from then on. (Run a Worker on the `reports` task
queue separately if you want the scheduled runs to actually execute.)

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java -Dexec.mainClass=training.temporal.schedules.CreateSchedule
```

Entry point: `java/src/main/java/training/temporal/schedules/CreateSchedule.java`.

## Python (`temporalio`)

```bash
cd python
uv run main.py
```

Entry point: `python/main.py` (Workflow in `python/report.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (Workflow in `go/report.go`).

## Expected output

```
Created Schedule 'daily-sales-report-schedule' (daily at 09:00, overlap=SKIP).
```

Inspect or clean up the Schedule with the CLI:

```bash
temporal schedule describe --schedule-id daily-sales-report-schedule
temporal schedule delete   --schedule-id daily-sales-report-schedule
```

In the Web UI the Schedule shows its next run time and, after each fire, the
`Recent Actions` list. Because the calendar spec is identical across SDKs, all
three create the same server-side Schedule.
