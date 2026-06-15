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

  <details><summary>Under the hood — what <code>make kafka-topic</code> runs</summary>

  ```bash
  docker exec temporal-training-kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic orders --partitions 4 --replication-factor 1
  ```

  </details>

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

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference solution: [`examples/runnable/05-kafka-bridge/python`](../../examples/runnable/05-kafka-bridge/python)
and [`.../go`](../../examples/runnable/05-kafka-bridge/go). Try the TODOs yourself
before peeking.

**Python** (`temporalio` + `kafka-python`) — a class-based producer Activity, a
Signal-fed Workflow, and a consumer bridge that uses `start_signal`:

```python
from datetime import timedelta
from temporalio import activity, workflow

class OutcomeActivities:
    def __init__(self, bootstrap_servers: str, topic: str):
        from kafka import KafkaProducer
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True, acks="all",
            key_serializer=lambda s: s.encode(), value_serializer=lambda s: s.encode(),
        )

    @activity.defn
    async def publish_outcome(self, order_id: str, outcome: str) -> None:
        # TODO: block on the send so a failure raises and Temporal retries
        self._producer.send(self._topic, key=order_id, value=outcome).get(timeout=10)

@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._events: list[str] = []

    @workflow.run
    async def run(self, order_id: str) -> str:
        processed = 0
        while True:
            # TODO 1: await workflow.wait_condition(lambda: len(self._events) > 0)
            # TODO 2: drain self._events, calling execute_activity_method(
            #         OutcomeActivities.publish_outcome, args=[order_id, f"accepted:{order_id}:{e}"], ...)
            # TODO 3: after ~1000 processed, workflow.continue_as_new(order_id)
            raise NotImplementedError

    @workflow.signal
    def order_event(self, payload: str) -> None:
        self._events.append(payload)

# Bridge (plain consumer, NOT workflow code): poll the orders topic, then
#   await client.start_workflow(OrderWorkflow.run, order_id, id=f"order-{order_id}",
#       task_queue="orders", start_signal="order_event", start_signal_args=[value])
#   and consumer.commit() ONLY after start_workflow returns.
```

**Go** (`go.temporal.io/sdk` + `segmentio/kafka-go`) — a Signal-channel Workflow
and a `SignalWithStartWorkflow` bridge:

```go
type OutcomeActivities struct{ writer *kafka.Writer }

func (a *OutcomeActivities) PublishOutcome(ctx context.Context, orderID, outcome string) error {
    // TODO: WriteMessages with RequiredAcks=RequireAll; a failure returns err so Temporal retries
    return a.writer.WriteMessages(ctx, kafka.Message{Key: []byte(orderID), Value: []byte(outcome)})
}

func OrderWorkflow(ctx workflow.Context, orderID string) error {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
    eventCh := workflow.GetSignalChannel(ctx, "orderEvent")
    var a *OutcomeActivities
    for processed := 0; ; processed++ {
        var payload string
        eventCh.Receive(ctx, &payload) // blocks until the next signal
        // TODO 1: ExecuteActivity(ctx, a.PublishOutcome, orderID, "accepted:"+orderID+":"+payload).Get(ctx, nil)
        // TODO 2: after ~1000, return workflow.NewContinueAsNewError(ctx, OrderWorkflow, orderID)
        _ = payload
    }
}

// Bridge: reader.FetchMessage; c.SignalWithStartWorkflow(ctx, "order-"+id, "orderEvent",
//   value, StartWorkflowOptions{ID: "order-"+id, TaskQueue: "orders"}, OrderWorkflow, id);
//   then reader.CommitMessages(ctx, msg) ONLY after the signal lands.
```

The rules are identical in all three SDKs: **`signalWithStart` (not start) keyed
by the record key, and commit the offset only after the signal is durably
accepted.**

</details>

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
```

<details><summary>Under the hood — what <code>make run-kafka</code> runs</summary>

```bash
cd examples/runnable/05-kafka-bridge && \
  mvn -q compile exec:java -Dexec.mainClass=training.temporal.kafka.KafkaWorker
```

</details>

```bash
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
