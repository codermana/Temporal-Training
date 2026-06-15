# Determinism & replay testing — runnable lab (Java · Python · Go)

The same data-pipeline Workflow in three SDKs, plus a "harmless looking" refactor
that **reorders** its two Activities. Each lab records a real history, then
replays it against both versions:

- the **original** code replays clean (history still matches), and
- the **reordered** code fails with a non-determinism error.

That failure is the regression replay testing catches *before* you deploy a
change to Workflows with executions already running in production.

The teaching point is identical everywhere: changing the **command stream**
(adding, removing, or reordering Activities/timers/signals) breaks replay.

## Java (`io.temporal:temporal-testing`)

```bash
cd java
mvn -q test
# or, from the repo root:  make run-replay
```

Records history in-process via `TestWorkflowEnvironment`, then
`WorkflowReplayer.replayWorkflowExecution(history, Impl.class)` — clean for the
original, throws for the reordered impl. Test:
`java/src/test/java/training/temporal/replay/ReplayDeterminismTest.java`.

## Python (`temporalio.worker.Replayer`)

```bash
cd python
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

Records history with `WorkflowEnvironment.start_time_skipping()` +
`handle.fetch_history()`, then `Replayer(workflows=[...]).replay_workflow(history)`.
Tests: `python/test_replay.py`.

## Go (`go.temporal.io/sdk/worker.WorkflowReplayer`)

```bash
cd go
go test ./...
```

Records history against an in-process dev server
(`testsuite.StartDevServer`, downloaded by the SDK — no external server), collects
the events into a `*historypb.History`, then `WorkflowReplayer.ReplayWorkflowHistory`.
Both impls register under the **same** Workflow type name so the recorded history
is replayed against each. Test: `go/replay_test.go`.

## Expected output

```
Java:   2 tests green (clean replay + expected non-determinism)
Python: 2 passed
Go:     ok  training.temporal/replay
```

The "breaks replay" test **passing** means replay correctly *detected* the
non-determinism — that is the safety net working.
