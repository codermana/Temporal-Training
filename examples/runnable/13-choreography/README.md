# Choreography with Temporal

This lab shows Temporal as one durable participant in an event-choreographed
system. There is no Kafka dependency: the starter simulates a stream of domain
events and the bridge turns those events into Workflow Signals.

The novel bit: the Workflow is not a central company-wide orchestrator. It owns
only the order service's local state for one `orderId`, while events such as
`OrderPlaced`, `PaymentCaptured`, and `InventoryReserved` remain the boundary
contract between services.

## Run

Start Temporal first:

```bash
scripts/start-temporal.sh
```

Run a Worker in one terminal:

```bash
scripts/run-example.sh choreography java worker
scripts/run-example.sh choreography python worker
scripts/run-example.sh choreography go worker
```

Run the starter in another terminal:

```bash
scripts/run-example.sh choreography java starter
scripts/run-example.sh choreography python starter
scripts/run-example.sh choreography go starter
```

Watch the Web UI at http://127.0.0.1:8233 and open workflow
`order-choreo-1001`.

## What to Notice

- The starter sends events out of process, like a Kafka consumer would.
- Java and Go use atomic signal-with-start: first event starts the Workflow,
  later events signal the same execution.
- Python demonstrates the same bridge shape with start-then-signal fallback so
  the lab stays runnable with the common SDK surface.
- The Workflow waits on durable conditions. If the Worker stops, replay restores
  the events and continues from the correct state.

