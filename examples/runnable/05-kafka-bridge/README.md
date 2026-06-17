# Kafka → Temporal → Kafka bridge: runnable lab (Java · Python · Go)

The same order pipeline in three SDKs: a plain Kafka consumer **bridge** turns
records on the `orders` topic into Workflow Signals via `signalWithStart`, one
long-lived `OrderWorkflow` runs per order key, and a producer **Activity** writes
an outcome back to the `order-outcomes` topic. The teaching points are identical
everywhere: *the consumer is not Workflow code, commit offsets only after the
signal lands, and all Kafka I/O lives in the bridge or an Activity.*

This lab needs **both** a Temporal dev server and a Kafka broker:

```bash
make temporal       # terminal 1
make stack-kafka    # terminal 2: KRaft single broker on :9092
make kafka-topic TOPIC=orders PARTITIONS=4
make kafka-topic TOPIC=order-outcomes PARTITIONS=4
```

## Java (`io.temporal:temporal-sdk` + `org.apache.kafka:kafka-clients`)

```bash
cd java && mvn -q compile exec:java
# or, from the repo root:  make run-kafka
```

Entry point: `java/src/main/java/training/temporal/kafka/KafkaWorker.java`.

## Python (`temporalio` + `kafka-python`)

```bash
cd python
uv run worker.py
```

Entry point: `python/worker.py` (workflow + producer Activity in `python/orders.py`).

## Go (`go.temporal.io/sdk` + `segmentio/kafka-go`)

```bash
cd go
go mod tidy
go run .
```

Entry point: `go/main.go` (workflow + producer Activity in `go/orders.go`).

## Drive it

```bash
echo 'NEW:line-item-A' | kcat -b localhost:9092 -t orders -P -k "order-1"

# Watch the outcome appear:
kcat -b localhost:9092 -t order-outcomes -C -o end -f 'key=%k value=%s\n'
```

## Expected output

```
key=order-1 value=accepted:order-1:NEW:line-item-A
```

A **second** event with the same key (`-k "order-1"`) signals the **same**
Workflow execution: in the Web UI `order-order-1` shows one
`WorkflowExecutionStarted` followed by a `WorkflowExecutionSignaled` and an
Activity completion per event, never a duplicate Workflow per key.
