# Testing Workflows — runnable lab (Java · Python · Go)

The same reminder Workflow in three SDKs, exercised by a unit test that needs
**no Temporal server**. A one-day `Workflow.sleep` completes in milliseconds
because each SDK's test environment **skips time**. A second test mocks an
Activity so the orchestration logic is verified in isolation.

No dev server is required — every test runs fully in-process.

## Java (`io.temporal:temporal-testing`)

```bash
cd java
mvn -q test
# or, from the repo root:  make run-testing
```

`TestWorkflowEnvironment` runs the Workflow in-process and skips time. Test:
`java/src/test/java/training/temporal/testing/ReminderWorkflowTest.java`.

## Python (`temporalio.testing`)

```bash
cd python
uv run pytest -q
```

`WorkflowEnvironment.start_time_skipping()` downloads and runs an in-process test
server (no dev server needed) and auto-skips timers. Tests: `python/test_reminder.py`
(time-skip, Activity mock via a same-named fake `@activity.defn`, and a failure path).

## Go (`go.temporal.io/sdk/testsuite`)

```bash
cd go
go test ./...
```

`testsuite.TestWorkflowEnvironment` runs the Workflow in-process and skips time;
`env.OnActivity(...).Return(...)` is the testify-mock stand-in for the real
Activity. Tests: `go/reminder_test.go`.

## Expected output

```
Java:   BUILD SUCCESS  (tests green)
Python: 3 passed
Go:     ok  training.temporal/testing
```

The one-day sleep adds no wall-clock time in any of the three — total runtime is
dominated by toolchain/test-server startup, not the simulated day.
