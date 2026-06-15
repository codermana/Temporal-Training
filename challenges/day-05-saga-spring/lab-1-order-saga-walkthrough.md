# Lab 5.1 — Order saga walkthrough

**Time:** ~60 min · **Difficulty:** ★★ · **Stack:** Temporal dev server

## Scenario

An order goes through three steps that each touch a different system:
**authorize payment → reserve inventory → ship**. Any step can fail. If shipping
fails after payment and inventory succeeded, you must **undo** the earlier steps
— refund the payment and restore the inventory — or you've charged a customer for
goods they'll never receive. You'll build this saga with a compensation stack.

## Learning goals

- Implement forward steps and their compensating Activities.
- Build a compensation stack that unwinds in reverse on failure.
- Bound Activity retries so a permanent failure actually reaches compensation.
- Observe the difference between a clean run and a compensated run in the UI.

## Prerequisites

- Day 2 (Activities, retries) understood. `make temporal` running.

## Starter code

Module `training.temporal.saga` (reuse Lab 1.2 `pom.xml`; `artifactId` `saga`,
exec `mainClass` `training.temporal.saga.SagaWorker`).

**Given contracts:**

```java
// OrderSagaWorkflow.java
@WorkflowInterface
public interface OrderSagaWorkflow {
  @WorkflowMethod
  String process(String orderId);   // returns "COMPLETED" or "COMPENSATED"
}
```

```java
// OrderActivities.java
@ActivityInterface
public interface OrderActivities {
  @ActivityMethod String authorizePayment(String orderId);   // -> paymentId
  @ActivityMethod String reserveInventory(String orderId);   // -> reservationId
  @ActivityMethod void   ship(String orderId);

  @ActivityMethod void cancelPayment(String paymentId);          // compensation
  @ActivityMethod void restoreInventory(String reservationId);   // compensation
  @ActivityMethod void sendFailureNotification(String orderId, String reason);
}
```

**Activity impl** — provide simple implementations. Make `ship` **fail** when the
orderId contains `"fail"` so you can trigger compensation on demand:

```java
public class OrderActivitiesImpl implements OrderActivities {
  // authorizePayment -> "payment-"+orderId ; reserveInventory -> "reservation-"+orderId
  // ship: throw if orderId.toLowerCase().contains("fail")
  // compensations: log what they undo ; sendFailureNotification: log the reason
  // TODO: implement
}
```

**Workflow impl** — the heart of the lab:

```java
// OrderSagaWorkflowImpl.java
public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              // IMPORTANT: bound the retries or a permanent failure retries
              // forever and compensation never runs.
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofMillis(500))
                      .setMaximumAttempts(3)
                      .build())
              .build());

  @Override
  public String process(String orderId) {
    Deque<Runnable> compensations = new ArrayDeque<>();
    try {
      // TODO 1: authorizePayment; push () -> cancelPayment(paymentId) onto the stack
      // TODO 2: reserveInventory; push () -> restoreInventory(reservationId)
      // TODO 3: ship(orderId)
      // TODO 4: return "COMPLETED"
      throw new UnsupportedOperationException("TODO");
    } catch (RuntimeException failure) {
      // TODO 5: pop and run every compensation (reverse order is automatic with a stack)
      // TODO 6: sendFailureNotification(orderId, failure.getMessage())
      // TODO 7: return "COMPENSATED"
      throw new UnsupportedOperationException("TODO");
    }
  }
}
```

**Worker** — register impls on the `orders` Task Queue and stay alive so you can
start Workflows from the CLI.

## Tasks

1. Implement the Activities (with the `ship` failure trigger).
2. Implement the saga: forward steps push compensations; the catch block unwinds.
3. Run a **happy path** order and a **failing** order; compare their histories.

## Verification

```bash
make run-saga      # Worker on the "orders" queue
```

<details><summary>Under the hood — what <code>make run-saga</code> runs</summary>

```bash
cd examples/runnable/07-saga && mvn -q compile exec:java -Dexec.mainClass=training.temporal.saga.SagaWorker
```

</details>

```bash
# Happy path -> COMPLETED
temporal workflow start --task-queue orders --type OrderSagaWorkflow \
  --workflow-id order-OK --input '"order-1001"'

# Failing path -> COMPENSATED (ship throws, compensations run)
temporal workflow start --task-queue orders --type OrderSagaWorkflow \
  --workflow-id order-fail --input '"fail-at-ship"'

temporal workflow show --workflow-id order-fail --output json \
  | jq -r '.events[] | select(.eventType|test("ActivityTask")) | .eventType'
```

Expected: `order-OK` returns `COMPLETED`; `order-fail` returns `COMPENSATED`, and
its history shows `cancelPayment` and `restoreInventory` Activities executing
after `ship` exhausts its retries.

## Definition of done

- [ ] Happy path completes through all three forward steps.
- [ ] Failing path runs compensations in **reverse** order, then notifies.
- [ ] Retries are bounded so the permanent `ship` failure reaches the catch block.
- [ ] You can point to the compensation Activities in the failed run's history.

## Pitfalls

- **Unbounded retries.** Without `setMaximumAttempts`, the default policy retries
  `ship` forever — the saga never reaches compensation. This is the #1 mistake.
- **Pushing the compensation before the forward step succeeds.** Only push a
  compensation *after* its forward step returns, or you'll try to undo something
  that never happened.
- Compensation Activities should be **idempotent** — they may themselves be
  retried.

## Hints

<details><summary>Hint 1 — the stack pattern</summary>

```java
String paymentId = activities.authorizePayment(orderId);
compensations.push(() -> activities.cancelPayment(paymentId));
```
On failure: `while (!compensations.isEmpty()) compensations.pop().run();` — a
`Deque` used as a stack unwinds in reverse automatically.
</details>

<details><summary>Hint 2 — Temporal's Saga helper</summary>

The SDK also ships `io.temporal.workflow.Saga`, which manages the compensation
list for you (`saga.addCompensation(...)` / `saga.compensate()`). Build it by
hand first to understand it, then try the helper as a refactor.
</details>

## Stretch goals

- Refactor to use the SDK's `Workflow`/`Saga` helper and compare readability.
- Make a **compensation** itself fail and decide the policy: retry forever, or
  escalate to a human via Signal?
- Add a `@QueryMethod` exposing which compensations have run so far.
