from dataclasses import dataclass

from temporalio.client import Client

from saga_compensation import OrderSagaWorkflow

# Spring's @KafkaListener is Java-only; the idiomatic Python analogue is a plain
# consumer loop (aiokafka here, in pseudocode) whose handler calls the Temporal
# client. The trigger contains no saga logic — it just starts the workflow.
#
# start_workflow with start_signal keeps Kafka redeliveries idempotent: the
# workflow is started once per order_id, and later events for the same key signal
# the already-running execution instead of crashing (signal-with-start).


@dataclass
class OrderRequest:
    order_id: str


async def on_order(client: Client, request: OrderRequest) -> None:
    await client.start_workflow(
        OrderSagaWorkflow.process,
        request.order_id,
        id=f"order-{request.order_id}",
        task_queue="orders",
        start_signal="on_update",
        start_signal_args=[request],
    )


async def consume(client: Client) -> None:
    # Pseudocode: a real consumer would use aiokafka and decode each message.
    #
    #   consumer = AIOKafkaConsumer("orders", bootstrap_servers="localhost:9092")
    #   async for msg in consumer:
    #       await on_order(client, decode(msg.value))
    ...
