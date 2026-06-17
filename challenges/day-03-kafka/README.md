# Day 3: Kafka integration & event-driven patterns

Today you bridge Kafka and Temporal: consume order events, drive a Workflow per
order key, produce outcome events back to Kafka, and fan out partition work
inside a Workflow. This replaces the "Kafka topic → REST trigger → Airflow DAG"
chain with a direct, durable path.

## Required stack

Temporal **and** a Kafka broker:

```bash
make temporal       # terminal 1
make stack-kafka    # terminal 2: KRaft single broker on :9092
```

Tear down with `make stack-down` (or `make day-3-down`) when finished.

Helper targets you'll use:

```bash
make kafka-topic TOPIC=orders PARTITIONS=4
make run-kafka                                   # the Worker + bridge
```

`kcat` is the simplest producer/consumer for poking topics; install it with the
full setup (`make setup-mac-full` / `make setup-ubuntu-full`).

## Labs

| # | Lab | Time | Difficulty |
|---|-----|------|-----------|
| 1 | [Kafka → Temporal → Kafka pipeline](lab-1-kafka-pipeline.md) | 70 min | ★★ |
| 2 | [Fan-out with Kafka partitions](lab-2-partition-fanout.md) | 45 min | ★★★ |

## Architecture in one picture

```
  orders topic ──▶ KafkaSignalBridge ──signalWithStart──▶ OrderWorkflow (per key)
   (consumer)        (plain Java thread)                       │
                                                               ▼
                                                    publishOutcome Activity
                                                               │
                                                               ▼
                                                      order-outcomes topic
```

Key idea: the **consumer is a plain Java thread** (the "bridge"), not Workflow
code. It commits offsets only after `signalWithStart` succeeds, giving
at-least-once delivery into durable Workflows. Workflow code never touches a
Kafka client directly; all I/O is in Activities or the bridge.

## Coming from Airflow / Kafka?

- **Kafka-triggered DAG → Signal-driven Workflow:** drop the REST trigger layer;
  the bridge signals the Workflow directly.
- **Consumer group offset commit → "commit after the Signal lands":** offsets
  advance only once Temporal has durably accepted the event.
- **DLQ vs retry exhaustion:** let Temporal own per-Activity retries; route to a
  Kafka DLQ only for genuinely un-processable messages (poison pills).
