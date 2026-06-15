# Lab 3.2 — Fan-out with Kafka partitions

**Time:** ~45 min · **Difficulty:** ★★★ · **Stack:** Temporal + Kafka

## Scenario

A batch job must process every partition of a topic and aggregate the results.
Instead of N separate consumers, you'll drive the parallelism **inside a
Workflow**: fan out one Activity per partition range, run them concurrently
(capped), and fan the results back into a single total. This combines Day 2's
`Promise.allOf` pattern with partition-aware work.

## Learning goals

- Fan out per-partition Activities with `Async.function` + `Promise.allOf`.
- **Cap concurrency** to avoid CPU spikes on a laptop (4–6 in-flight).
- Aggregate partial results deterministically in the parent Workflow.

> **Coming from Kafka `[kafka]`:** you're not adding consumers to a group; you're
> orchestrating bounded parallel work over partition ranges from one durable
> Workflow, with automatic retry per branch.

## Prerequisites

- Lab 3.1 patterns understood. `make temporal` + `make stack-kafka` running.
- A topic with several partitions:

  ```bash
  make kafka-topic TOPIC=batch-input PARTITIONS=6
  ```

  <details><summary>Under the hood — what <code>make kafka-topic</code> runs</summary>

  ```bash
  docker exec temporal-training-kafka \
    /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic batch-input --partitions 6 --replication-factor 1
  ```

  </details>

  Produce some keyed records so partitions are non-empty (any `kcat -P` loop).

## Starter code

New module (or extend Lab 3.1's). Sketch the contracts yourself this time —
you've seen the pattern. Target shape:

```java
@WorkflowInterface
public interface PartitionFanoutWorkflow {
  @WorkflowMethod
  long processAllPartitions(String topic, int partitionCount);
}

@ActivityInterface
public interface PartitionActivities {
  @ActivityMethod
  long processPartition(String topic, int partition);   // returns a per-partition count/sum
}
```

```java
// PartitionFanoutWorkflowImpl.java
public class PartitionFanoutWorkflowImpl implements PartitionFanoutWorkflow {
  // activity stub with a startToCloseTimeout + bounded retries

  @Override
  public long processAllPartitions(String topic, int partitionCount) {
    // TODO 1: fan out processPartition for each partition, but cap the number of
    //         simultaneously in-flight Activities at MAX_PARALLEL (4..6).
    // TODO 2: join each batch with Promise.allOf(...).get().
    // TODO 3: sum all per-partition results and return the total.
    throw new UnsupportedOperationException("TODO");
  }
}
```

The Activity impl can consume that partition with a `KafkaConsumer` assigned to
the specific `TopicPartition` (use `assign`, not `subscribe`) and return a count.
Keep it simple — the focus is the fan-out, not the consumer.

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

The capped fan-out pattern from [`examples/04-kafka`](../../examples/04-kafka)
(`partition_fanout.py` / `.go`) — process partitions in chunks of `MAX_PARALLEL`
so no more than that many Activities are in flight at once.

**Python** (`temporalio`) — chunked `asyncio.gather`:

```python
import asyncio
from datetime import timedelta
from temporalio import activity, workflow

MAX_PARALLEL = 4

@activity.defn
async def process_partition(topic: str, partition: int) -> int:
    # TODO: assign a KafkaConsumer to (topic, partition) and return a count
    raise NotImplementedError

@workflow.defn
class PartitionFanoutWorkflow:
    @workflow.run
    async def process_all_partitions(self, topic: str, partition_count: int) -> int:
        total = 0
        for start in range(0, partition_count, MAX_PARALLEL):
            chunk = range(start, min(start + MAX_PARALLEL, partition_count))
            counts = [
                workflow.execute_activity(
                    process_partition, args=[topic, p],
                    start_to_close_timeout=timedelta(minutes=20),
                )
                for p in chunk
            ]
            # TODO: await asyncio.gather(*counts) and add to total (in chunk order)
        return total
```

**Go** (`go.temporal.io/sdk`) — collect Futures per chunk, then `Get` each:

```go
const MaxParallel = 4

func ProcessPartition(ctx context.Context, topic string, partition int) (int, error) {
    // TODO: assign a reader to (topic, partition) and return a count
    return 0, nil
}

func PartitionFanoutWorkflow(ctx workflow.Context, topic string, partitionCount int) (int, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 20 * time.Minute})
    total := 0
    for start := 0; start < partitionCount; start += MaxParallel {
        end := start + MaxParallel
        if end > partitionCount {
            end = partitionCount
        }
        var futures []workflow.Future
        for p := start; p < end; p++ {
            futures = append(futures, workflow.ExecuteActivity(ctx, ProcessPartition, topic, p))
        }
        // TODO: Get each Future in order and add to total
        _ = futures
    }
    return total, nil
}
```

The cap is the point: launch a chunk, join it, accumulate, then start the next —
never all partitions at once. Sum in a fixed (partition) order so the result is
replay-stable.

</details>

## Tasks

1. Implement `processPartition` to read one partition and return a number.
2. Implement the **capped** fan-out in the Workflow: never have more than
   `MAX_PARALLEL` (4–6) Activities in flight at once.
3. Aggregate and return the grand total.
4. Run it against a 6-partition topic and confirm the total matches what you
   produced.

## Verification

Start the Workflow (via a small `main` or `temporal workflow start`) with
`topic=batch-input, partitionCount=6`. Expected: the returned total equals the
number of records you produced. In the Web UI history, confirm **at most
`MAX_PARALLEL`** `ActivityTaskScheduled` events are outstanding before earlier
ones complete — the cap is visible in the scheduling pattern.

## Definition of done

- [ ] One Activity processes one partition; results aggregate to a correct total.
- [ ] Concurrency is capped at 4–6 in-flight, demonstrably (history shows
      batching, not all 6+ at once if cap < partitions).
- [ ] The Workflow is deterministic — no partition iteration order leaks
      non-determinism into the result.

## Pitfalls

- **Unbounded fan-out** of dozens of partitions at once spikes CPU on training
  laptops. The cap is the point of this lab.
- **Aggregation order.** Sum from the resolved promises in a fixed order; don't
  rely on completion order, which varies between runs/replays.
- Assign the consumer to a specific `TopicPartition` (`consumer.assign(...)`),
  not a subscription — you want *this* Activity to own *this* partition.

## Hints

<details><summary>Hint 1 — capping concurrency</summary>

Process partitions in chunks of `MAX_PARALLEL`: launch a chunk with
`Async.function`, `Promise.allOf(chunk).get()`, accumulate, then start the next
chunk. Or maintain a sliding window of in-flight promises.
</details>

<details><summary>Hint 2 — deterministic sum</summary>

Keep your `List<Promise<Long>>` in partition order and sum
`promises.stream().mapToLong(Promise::get).sum()` after joining — the values are
fixed once recorded, so the total is replay-stable.
</details>

## Stretch goals

- Make one partition's Activity fail permanently and decide the policy: fail the
  whole batch, or skip and report which partitions succeeded?
- Compare wall-clock time at `MAX_PARALLEL = 1` vs `4` vs `6` and relate it to
  available cores.
