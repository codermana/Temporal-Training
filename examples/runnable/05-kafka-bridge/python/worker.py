"""Run the Worker and the Kafka signal bridge, mirroring KafkaWorker.java.

    python worker.py        # needs a Temporal dev server + a Kafka broker

The bridge is a plain Kafka consumer (NOT Workflow code). It commits offsets only
after start_workflow's start_signal lands — at-least-once delivery into Temporal.
"""

import asyncio
import os

from kafka import KafkaConsumer
from temporalio.client import Client
from temporalio.worker import Worker

from orders import TASK_QUEUE, OrderWorkflow, OutcomeActivities


async def bridge(client: Client, bootstrap_servers: str, topic: str) -> None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id="temporal-order-bridge",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        key_deserializer=lambda b: b.decode() if b else "",
        value_deserializer=lambda b: b.decode(),
    )
    loop = asyncio.get_running_loop()
    try:
        while True:
            # poll() blocks; run it off the event loop so the Worker keeps running.
            batches = await loop.run_in_executor(None, lambda: consumer.poll(timeout_ms=1000))
            for _tp, records in batches.items():
                for record in records:
                    order_id = record.key
                    # start_signal == Java signalWithStart: start on the first
                    # event for a key, signal the existing run for later events.
                    await client.start_workflow(
                        OrderWorkflow.run,
                        order_id,
                        id=f"order-{order_id}",
                        task_queue=TASK_QUEUE,
                        start_signal="order_event",
                        start_signal_args=[record.value],
                    )
            if batches:
                # Commit ONLY after the signals landed.
                consumer.commit()
    finally:
        consumer.close()


async def main() -> None:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    input_topic = os.environ.get("KAFKA_ORDERS_TOPIC", "orders")
    outcome_topic = os.environ.get("KAFKA_OUTCOMES_TOPIC", "order-outcomes")

    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    )
    activities = OutcomeActivities(bootstrap_servers, outcome_topic)

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[activities.publish_outcome],
    ):
        print(
            f"Kafka bridge running. brokers={bootstrap_servers} in={input_topic} "
            f"out={outcome_topic} taskQueue={TASK_QUEUE}. Ctrl+C to stop."
        )
        await bridge(client, bootstrap_servers, input_topic)


if __name__ == "__main__":
    asyncio.run(main())
