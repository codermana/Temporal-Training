# Lab 2.1 — Async & parallel Activities

**Time:** ~50 min · **Difficulty:** ★★ · **Stack:** Temporal dev server

## Scenario

An order contains several line-item SKUs. Pricing each SKU is an independent
Activity call. Done sequentially, an order with 8 SKUs takes 8× one call. You'll
fan these out to run concurrently and fan the results back in to a total — the
bread-and-butter parallelism pattern.

## Learning goals

- Invoke Activities asynchronously with `Async.function(...)`.
- Collect concurrent results with `Promise.allOf(...)` and `Promise.get()`.
- Understand fan-out/fan-in and how partial failures surface.

> **Coming from Airflow `[airflow]`:** this is a dynamic `TaskGroup` of parallel
> tasks whose count depends on runtime input — without writing a custom operator
> or mapping plugin. It's just a `stream().map(...)` in Workflow code.

## Prerequisites

- Day 1 complete; `make temporal` running.

<details><summary>Under the hood — what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
```

gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
Overridable via env: `TEMPORAL_HOST`, `TEMPORAL_PORT`, `TEMPORAL_UI_PORT`, `TEMPORAL_METRICS_PORT`.

</details>

## Starter code

Scaffold a module like Lab 1.2 (reuse that `pom.xml`, change `artifactId` to
`async-parallel` and the exec `mainClass` to `training.temporal.parallel.PricingWorker`).
Package `training.temporal.parallel`.

**Given contracts** (paste as-is):

```java
// OrderPricingWorkflow.java
package training.temporal.parallel;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.List;

@WorkflowInterface
public interface OrderPricingWorkflow {
  @WorkflowMethod
  int total(List<String> skus);
}
```

```java
// PricingActivities.java
package training.temporal.parallel;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface PricingActivities {
  @ActivityMethod
  int price(String sku);
}
```

**Activity impl** — complete it (a small price lookup is fine):

```java
// PricingActivitiesImpl.java
package training.temporal.parallel;

import io.temporal.activity.Activity;

public class PricingActivitiesImpl implements PricingActivities {
  @Override
  public int price(String sku) {
    // TODO: heartbeat (Activity.getExecutionContext().heartbeat(...)) then
    //       return a price for the sku. A small Map lookup with a default is fine.
    throw new UnsupportedOperationException("TODO");
  }
}
```

**Workflow impl** — the core of the lab:

```java
// OrderPricingWorkflowImpl.java
package training.temporal.parallel;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.List;

public class OrderPricingWorkflowImpl implements OrderPricingWorkflow {

  private final PricingActivities activities =
      Workflow.newActivityStub(
          PricingActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder().setMaximumAttempts(3).build())
              .build());

  @Override
  public int total(List<String> skus) {
    // TODO 1: for each sku, kick off an async Activity call -> List<Promise<Integer>>.
    // TODO 2: wait for ALL of them to finish.
    // TODO 3: sum the resolved prices and return the total.
    throw new UnsupportedOperationException("TODO");
  }
}
```

**Worker/starter** — fill the registration + start (same shape as Lab 1.2's
`HelloWorker`). Start the Workflow with, e.g., `List.of("book","lamp","desk")`
and print the total.

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference solution: [`examples/runnable/02-async-parallel-activities/python`](../../examples/runnable/02-async-parallel-activities/python)
and [`.../go`](../../examples/runnable/02-async-parallel-activities/go). Try the
TODOs yourself before peeking.

**Python** (`temporalio`) — fan out with `asyncio.gather`:

```python
from datetime import timedelta
import asyncio
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

@activity.defn
async def price(sku: str) -> int:
    activity.heartbeat(f"pricing {sku}")
    # TODO: return a price for the sku (a dict lookup with a default is fine)
    raise NotImplementedError

@workflow.defn
class OrderPricingWorkflow:
    @workflow.run
    async def total(self, skus: list[str]) -> int:
        # TODO 1: start one execute_activity(price, sku, ...) per sku (don't await yet)
        # TODO 2: await asyncio.gather(*...) to join them all
        # TODO 3: return the sum
        raise NotImplementedError
```

**Go** (`go.temporal.io/sdk`) — collect Futures, then `Get` each:

```go
func Price(ctx context.Context, sku string) (int, error) {
    activity.RecordHeartbeat(ctx, "pricing "+sku)
    // TODO: return a price for the sku
    return 0, nil
}

func OrderPricingWorkflow(ctx workflow.Context, skus []string) (int, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
        StartToCloseTimeout: 30 * time.Second,
        RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
    })
    // TODO 1: start one workflow.ExecuteActivity(ctx, Price, sku) per sku, collecting Futures
    // TODO 2: Get each Future and sum
    return 0, nil
}
```

The parallelism rule is identical in all three SDKs: **start every Activity
before you wait on any of them.** Awaiting inside the loop serializes them.

</details>

## Tasks

1. Implement `price(...)` with a heartbeat and a price lookup.
2. In `total(...)`: map each SKU to `Async.function(activities::price, sku)`,
   collect into a `List<Promise<Integer>>`, block on `Promise.allOf(...).get()`,
   then sum each promise's value.
3. Register and run; print the total.
4. Open the Web UI and confirm the Activities ran **concurrently** (their
   scheduled/started times overlap), not one after another.

## Verification

```bash
mvn -q compile exec:java
```

Expected: prints the summed total (e.g. `Total price: 355` for book+lamp+desk).
In the Web UI history you should see all `ActivityTaskScheduled` events emitted
in the **same** Workflow Task, before any results come back.

## Definition of done

- [ ] All SKU prices are fetched via `Async.function`, not a sequential loop of
      blocking calls.
- [ ] `Promise.allOf(...).get()` is used to join before summing.
- [ ] The history shows the Activities scheduled together (parallel fan-out).

## Pitfalls

- Calling `activities.price(sku)` directly inside the loop makes it
  **sequential** — each call blocks until it returns. You must use
  `Async.function` to get concurrency.
- Don't `.get()` each promise inside the loop either — that also serializes
  them. Schedule them all first, then join.

## Hints

<details><summary>Hint 1 — the fan-out one-liner</summary>

```java
List<Promise<Integer>> prices =
    skus.stream().map(sku -> Async.function(activities::price, sku)).toList();
```
Then `Promise.allOf(prices).get();` and sum with
`prices.stream().mapToInt(Promise::get).sum();`.
</details>

<details><summary>Hint 2 — partial failure behavior</summary>

If one SKU's Activity exhausts its retries, `Promise.allOf(...).get()` throws.
That's the fail-fast default. The stretch goal explores tolerating it.
</details>

## Stretch goals

- **Tolerate partial failure.** Make one SKU always fail and change the Workflow
  to skip failed line items and total the rest. (Hint: don't join with
  `allOf`; resolve each promise in a try/catch.)
- **Race instead of join.** Use `Promise.anyOf(...)` to return as soon as the
  first price resolves — when would you want that?
- **Cancellation.** Wrap the fan-out in a `CancellationScope` and cancel the
  remaining calls once you have "enough" prices.
