# Task-queue routing: runnable demo (Java · Python · Go)

One Workflow whose Activities run on **different Worker pools**, chosen by Task
Queue. The teaching point: *the unit of "who registers what" is the Task Queue,
not the Worker* (no single Worker registers the full set of Activities).

Topology:

```text
                   Task Queue        Worker pool (registers)
  OrderWorkflow ─▶ "orders"   ─────▶ Orchestrator [OrderWorkflow]
       │
       ├ charge ─▶ "payments" ─────▶ Payments pool [PaymentActivities]
       └ render ─▶ "media"    ─────▶ Media pool    [MediaActivities]  (GPU)
```

The `OrderWorkflow` runs on `orders`; its `charge` Activity stub names the
`payments` queue and its `render` stub names the `media` queue. The Worker
process starts all three pools (one per Task Queue), each registering only its
subset; in production each pool would be its own deployment.

The Worker and the client (starter) are separate, standalone processes. Run the
Worker in one terminal and the starter in another. Both connect to a local
Temporal dev server.

Start Temporal first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

## Java (`io.temporal:temporal-sdk`)

```bash
scripts/run-example.sh routing java worker     # terminal 1
scripts/run-example.sh routing java starter    # terminal 2
```

Entry points: `java/.../routing/RoutingWorker.java` and `RoutingStarter.java`.

## Python (`temporalio`)

```bash
scripts/run-example.sh routing python worker   # terminal 1
scripts/run-example.sh routing python starter  # terminal 2
```

Entry points: `python/worker.py` and `python/starter.py`.

## Go (`go.temporal.io/sdk`)

```bash
scripts/run-example.sh routing go worker       # terminal 1
scripts/run-example.sh routing go starter      # terminal 2
```

Entry points: `go/worker/main.go` and `go/starter/main.go`.

## Expected output

The starter prints:

```text
Workflow result: order-1001: charged $42.00 / s3://receipts/order-1001.pdf
```

In the **Worker** terminal, two different pools log independently, proof the
Activities were routed to separate Workers:

```text
[payments pool] charging order order-1001
[media pool] rendering receipt for order order-1001
```

In the Web UI history, the two `ActivityTaskScheduled` events carry different
`taskQueue` values (`payments` and `media`), while the Workflow itself ran on
`orders`.
