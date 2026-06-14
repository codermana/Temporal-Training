# Lab 3.1 — Kafka → Temporal → Kafka pipeline

**Time:** ~70 min · **Difficulty:** ★★ · **Stack:** Temporal + Kafka

## Scenario

Order events land on a Kafka `orders` topic. Each order should drive a durable
Workflow that processes the event and publishes an outcome to an
`order-outcomes` topic. You'll build the full path: a consumer **bridge** that
turns Kafka records into Workflow Signals, a long-lived `OrderWorkflow` per order
key, and a producer **Activity** that writes the outcome back.

## Learning goals

- Bridge a Kafka consumer to Temporal using `signalWithStart`.
- Run one long-lived Workflow per entity key, fed by Signals.
- Produce to Kafka from an Activity with an idempotent producer.
- Commit offsets only after the Signal is durably accepted (at-least-once).
- Keep history bounded with `continueAsNew`.

## Prerequisites

- Day 2 (Signals) complete. `make temporal` + `make stack-kafka` running.
- Create the topics:

  ```bash
  make kafka-topic TOPIC=orders PARTITIONS=4
  make kafka-topic TOPIC=order-outcomes PARTITIONS=4
  ```

## Starter code

Scaffold a module in `training.temporal.kafka`. The `pom.xml` needs the Temporal
SDK **plus** the Kafka client:

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>3.7.0</version>
</dependency>
```

(Keep the `temporal-sdk` and `slf4j-simple` deps from Lab 1.2; set exec
`mainClass` to `training.temporal.kafka.KafkaWorker`.)

**Given contracts:**

```java
// OrderWorkflow.java
package training.temporal.kafka;

import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderWorkflow {
  @WorkflowMethod
  String run(String orderId);

  @SignalMethod
  void orderEvent(String payload);
}
```

```java
// OutcomeActivities.java
package training.temporal.kafka;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface OutcomeActivities {
  @ActivityMethod
  void publishOutcome(String orderId, String outcome);
}
```

**Four files to complete** (stubs):

```java
// OrderWorkflowImpl.java — long-lived, one per order key
package training.temporal.kafka;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

public class OrderWorkflowImpl implements OrderWorkflow {
  private final OutcomeActivities activities =
      Workflow.newActivityStub(
          OutcomeActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  private final Deque<String> events = new ArrayDeque<>();

  @Override
  public String run(String orderId) {
    // TODO: loop forever:
    //   - await until `events` is non-empty
    //   - drain each event, calling publishOutcome(orderId, "accepted:"+...)
    //   - after ~1000 processed events, continueAsNew(orderId) to bound history
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void orderEvent(String payload) {
    // TODO: enqueue the payload for run() to process.
  }
}
```

```java
// KafkaOutcomeActivities.java — the producer Activity impl
package training.temporal.kafka;

// TODO: implement OutcomeActivities.publishOutcome by producing a record to the
//       outcomes topic with a KafkaProducer. Configure:
//         ENABLE_IDEMPOTENCE_CONFIG=true, ACKS_CONFIG=all, String serializers.
//       Block on the send (.get()) so a failure throws and Temporal retries.
```

```java
// KafkaSignalBridge.java — plain Java consumer thread (NOT workflow code)
package training.temporal.kafka;

// TODO: implements Runnable. In run():
//   - subscribe a KafkaConsumer (enable.auto.commit=false) to the orders topic
//   - poll; for each record, build an OrderWorkflow stub with workflowId
//     "order-"+record.key() on the orders task queue
//   - use client.newSignalWithStartRequest() / BatchRequest to add
//     (workflow::run, orderId) AND (workflow::orderEvent, value), then
//     client.signalWithStart(batch)
//   - commitSync() ONLY after signalWithStart returns
```

```java
// KafkaWorker.java — entrypoint
package training.temporal.kafka;

// TODO: start a Worker on the "orders" task queue registering OrderWorkflowImpl
//       and a KafkaOutcomeActivities(bootstrap, outcomesTopic). Start a daemon
//       thread running KafkaSignalBridge. Keep alive with CountDownLatch.
//       Read bootstrap/topics from env with sensible localhost defaults.
```

## Tasks

1. Implement the producer Activity (`KafkaOutcomeActivities`).
2. Implement `OrderWorkflowImpl` (await → drain → publish → continue-as-new).
3. Implement the `KafkaSignalBridge` consumer thread.
4. Wire `KafkaWorker` and run it.
5. Produce an order event and watch an outcome event appear.

## Verification

```bash
make run-kafka      # starts Worker + bridge

# In another terminal: produce an order, key = order id
echo 'NEW:line-item-A' | kcat -b localhost:9092 -t orders -P -k "order-1"

# Consume outcomes
kcat -b localhost:9092 -t order-outcomes -C -o end -f 'key=%k value=%s\n'
```

Expected: an outcome like `key=order-1 value=accepted:order-1:NEW:line-item-A`.
In the Web UI, `order-order-1` shows a `WorkflowExecutionStarted` followed by
`WorkflowExecutionSignaled` and an Activity completion per event.

## Definition of done

- [ ] Producing to `orders` causes a matching record on `order-outcomes`.
- [ ] A second event for the **same** key signals the **same** Workflow
      execution (no duplicate Workflow per key).
- [ ] Offsets commit only after `signalWithStart` succeeds.
- [ ] The Workflow uses `continueAsNew` to keep history bounded.

## Pitfalls

- **Don't run a Kafka client inside Workflow code.** Consuming/producing is I/O —
  it belongs in the bridge thread (consume) and an Activity (produce). Workflow
  code stays deterministic.
- **`signalWithStart`, not `start`.** The first event for a key must start the
  Workflow *and* deliver the event atomically; later events must signal the
  existing one. Plain `start` throws "already started" on the second event.
- **Commit ordering.** Commit *after* the signal lands, or a crash between
  consume and signal silently drops the event.

## Hints

<details><summary>Hint 1 — signalWithStart shape</summary>

```java
BatchRequest batch = client.newSignalWithStartRequest();
batch.add(workflow::run, orderId);
batch.add(workflow::orderEvent, record.value());
client.signalWithStart(batch);
```
Build the typed stub with a `workflowId` derived from `record.key()` so the same
key always maps to the same execution.
</details>

<details><summary>Hint 2 — bounding history</summary>

Count processed events; when the count crosses a threshold (e.g. 1000), call
`Workflow.continueAsNew(orderId)`. The new run starts with empty history and an
empty queue, but the same Workflow ID continuity.
</details>

## Stretch goals

- Make the outcome reflect real processing (e.g. reject events containing
  `"FAIL"`); produce `rejected:` outcomes and confirm retries vs. terminal
  failures behave as you expect.
- Add an idempotency key so a redelivered Kafka record (at-least-once) doesn't
  double-process within a Workflow run.
