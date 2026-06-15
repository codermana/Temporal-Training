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

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference solution: [`examples/runnable/07-saga/python`](../../examples/runnable/07-saga/python)
and [`.../go`](../../examples/runnable/07-saga/go). Try the TODOs yourself before
peeking. Neither SDK has a built-in `Saga` helper — you keep the compensation
stack by hand (a list/slice) and unwind it in reverse on failure.

**Python** (`temporalio`) — manual compensation stack with try/except:

```python
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

@activity.defn
async def ship(order_id: str) -> None:
    if "fail" in order_id.lower():            # fail on demand
        raise RuntimeError("shipping label service failed")
# authorize_payment -> "payment-"+id ; reserve_inventory -> "reservation-"+id
# cancel_payment / restore_inventory / send_failure_notification: log what they undo

@workflow.defn
class OrderSagaWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> str:
        opts = dict(
            start_to_close_timeout=timedelta(seconds=30),
            # IMPORTANT: bound retries or a permanent failure retries forever
            # and compensation never runs.
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        compensations: list = []            # each entry: (activity_fn, arg)
        try:
            # TODO 1: authorize_payment; append (cancel_payment, payment_id)
            # TODO 2: reserve_inventory; append (restore_inventory, reservation_id)
            # TODO 3: await ship(order_id); return "COMPLETED"
            raise NotImplementedError
        except Exception as failure:
            # TODO 4: for fn, arg in reversed(compensations): execute_activity(fn, arg, **opts)
            # TODO 5: send_failure_notification(order_id, str(failure)); return "COMPENSATED"
            raise NotImplementedError
```

**Go** (`go.temporal.io/sdk`) — compensation slice run in reverse on error:

```go
func Ship(ctx context.Context, orderID string) error {
    if strings.Contains(strings.ToLower(orderID), "fail") {
        return errors.New("shipping label service failed")
    }
    return nil
}

func OrderSagaWorkflow(ctx workflow.Context, orderID string) (string, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        // IMPORTANT: bound retries or compensation never runs.
        RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3},
    })
    var compensations []func()
    compensate := func() {
        for i := len(compensations) - 1; i >= 0; i-- { compensations[i]() }
    }
    // TODO 1: AuthorizePayment; append a closure that runs CancelPayment(paymentID)
    // TODO 2: ReserveInventory; append a closure that runs RestoreInventory(reservationID)
    // TODO 3: Ship(orderID); on error -> compensate(), SendFailureNotification, return "COMPENSATED"
    // TODO 4: success -> return "COMPLETED"
    return "", nil
}
```

The rule is identical in all three SDKs: **register a compensation only after its
forward step succeeds, and unwind in reverse on failure.** Java's `Saga` helper
automates the stack; Python/Go do it by hand.

</details>

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
