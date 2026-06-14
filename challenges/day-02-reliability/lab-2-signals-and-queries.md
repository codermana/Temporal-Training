# Lab 2.2 — Signals & Queries

**Time:** ~45 min · **Difficulty:** ★★ · **Stack:** Temporal dev server

## Scenario

A purchase order needs human approval. The Workflow starts, then **waits** —
possibly for hours — until someone approves or rejects it. Meanwhile a dashboard
needs to read its current state without disturbing it. Signals deliver the
decision; Queries read the state. This is the canonical "durable wait for a
human" pattern that Airflow sensors fake with polling.

## Learning goals

- Block a Workflow on external input with `Workflow.await(...)`.
- Receive asynchronous input via `@SignalMethod`.
- Expose read-only state via `@QueryMethod` (no side effects allowed).
- Drive a running Workflow entirely from the CLI.

> **Coming from Airflow `[airflow]`:** no `ExternalTaskSensor` poll loop and no
> reschedule churn. The Workflow simply parks on `await` consuming zero
> resources until a Signal wakes it.

## Prerequisites

- Day 1 complete; `make temporal` running.

## Starter code

Scaffold a module in `training.temporal.approval` (reuse the Lab 1.2 `pom.xml`;
`artifactId` `approval`, exec `mainClass`
`training.temporal.approval.ApprovalWorker`).

**Given contract** — note it already includes Update methods you'll use in Lab
2.3. For *this* lab implement `run`, `approve`, `reject`, and `currentState`;
leave the update methods as stubs for now.

```java
// ApprovalWorkflow.java
package training.temporal.approval;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.UpdateMethod;
import io.temporal.workflow.UpdateValidatorMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface ApprovalWorkflow {
  @WorkflowMethod
  String run(String requestId);

  @SignalMethod
  void approve(String approver);

  @SignalMethod
  void reject(String reason);

  @QueryMethod
  String currentState();

  // Used in Lab 2.3 — leave unimplemented (throw) until then.
  @UpdateMethod
  String changeNote(String note);

  @UpdateValidatorMethod(updateName = "changeNote")
  void validateNote(String note);
}
```

**Workflow impl** — complete the signal/query/await logic:

```java
// ApprovalWorkflowImpl.java
package training.temporal.approval;

import io.temporal.workflow.Workflow;

public class ApprovalWorkflowImpl implements ApprovalWorkflow {
  private String requestId;
  private String status = "WAITING";
  private String note = "initial request";

  @Override
  public String run(String requestId) {
    this.requestId = requestId;
    // TODO: block until status becomes APPROVED or REJECTED, using Workflow.await(...).
    //       Then return a summary string (id + status + note).
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void approve(String approver) {
    // TODO: set status to indicate approval by `approver`.
  }

  @Override
  public void reject(String reason) {
    // TODO: set status to indicate rejection with `reason`.
  }

  @Override
  public String currentState() {
    // TODO: return the current state. MUST be read-only (no mutation, no Activities).
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String changeNote(String note) { throw new UnsupportedOperationException("Lab 2.3"); }

  @Override
  public void validateNote(String note) { throw new UnsupportedOperationException("Lab 2.3"); }
}
```

**Worker** — start a Worker on a task queue (e.g. `approval`), start one
Workflow with a fixed Workflow ID (e.g. `approval-demo`), then keep the process
alive (`new CountDownLatch(1).await();`) so you can drive it from the CLI.
Handle `WorkflowExecutionAlreadyStarted` so re-running the Worker reuses the
execution.

## Tasks

1. Implement `run` to park on `Workflow.await(() -> /* approved or rejected */)`
   and return a summary.
2. Implement `approve` and `reject` to set `status`.
3. Implement `currentState` as a pure read of the fields.
4. Start the Worker, then from another terminal **query** and **signal** it.

## Verification

With the Worker running (`approval-demo` started):

```bash
# Read state without disturbing the Workflow
temporal workflow query  --workflow-id approval-demo --type currentState

# Approve it
temporal workflow signal --workflow-id approval-demo --name approve \
  --input '"manager@example.com"'

# Query again — status changed; the Workflow then completes
temporal workflow query  --workflow-id approval-demo --type currentState
temporal workflow show   --workflow-id approval-demo --output json | jq -r '.events[].eventType'
```

Expected: the first query shows `WAITING`; after the signal the Workflow
completes and the history contains `WorkflowExecutionSignaled`.

## Definition of done

- [ ] Querying before a decision returns `WAITING` and leaves history unchanged.
- [ ] An `approve` (or `reject`) signal unblocks `run` and the Workflow
      completes.
- [ ] `currentState` performs no mutation and schedules no Activities.

## Pitfalls

- **Queries must be side-effect free.** No field writes, no Activity calls, no
  `Workflow.await`. A query that mutates state corrupts replay.
- **Don't busy-wait.** `while(!done){}` is wrong and non-deterministic — use
  `Workflow.await(condition)`, which yields until a signal changes the condition.
- A Query against a not-yet-started Workflow ID fails — start it first.

## Hints

<details><summary>Hint 1 — the await condition</summary>

```java
Workflow.await(() -> status.startsWith("APPROVED") || status.startsWith("REJECTED"));
```
A Signal handler mutating `status` is what makes the predicate flip.
</details>

<details><summary>Hint 2 — keeping the Worker alive to receive signals</summary>

After `factory.start()` and starting the Workflow, block the main thread with
`new CountDownLatch(1).await();`. Add a shutdown hook calling
`factory.shutdown()`.
</details>

## Stretch goals

- Add a **timeout to the wait**: if no decision arrives within N seconds
  (`Workflow.await(Duration, condition)`), auto-reject. Compare to an Airflow
  sensor `timeout`.
- Add a `@QueryMethod` that returns *structured* state (a small record) instead
  of a string, and inspect how the UI renders it.
