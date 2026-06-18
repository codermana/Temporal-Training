"""Kafka -> Temporal -> Kafka order pipeline (Python port of the Java lab).

One long-lived OrderWorkflow per order key, fed by Signals from a plain Kafka
consumer "bridge". The Workflow publishes an outcome per event through an
idempotent producer Activity. Workflow code never touches Kafka directly — all
I/O lives in the bridge (consume) and the Activity (produce).
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from kafka import KafkaProducer

TASK_QUEUE = "orders"


class OutcomeActivities:
    """Producer Activity. Idempotent + acks=all so Temporal retries can't
    duplicate outcome events on the broker."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            key_serializer=lambda s: s.encode(),
            value_serializer=lambda s: s.encode(),
        )

    @activity.defn
    async def publish_outcome(self, order_id: str, outcome: str) -> None:
        # Block on the send so a broker failure raises and Temporal retries.
        self._producer.send(self._topic, key=order_id, value=outcome).get(timeout=10)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self._events: list[str] = []

    # One long-lived Workflow per order key. The first Kafka event starts it; every
    # later event for the same key is delivered as a Signal to this same execution
    # (via start_signal in the bridge). It stays open, publishing an outcome per
    # event, and continue-as-news to keep its history bounded.
    @workflow.run
    async def run(self, order_id: str) -> str:
        processed = 0
        while True:
            await workflow.wait_condition(lambda: len(self._events) > 0)
            while self._events:
                event = self._events.pop(0)
                await workflow.execute_activity_method(
                    OutcomeActivities.publish_outcome,
                    args=[order_id, f"accepted:{order_id}:{event}"],
                    start_to_close_timeout=timedelta(seconds=10),
                )
                processed += 1
            if processed >= 1000:
                workflow.continue_as_new(order_id)

    # External signal name matches the Java/Go bridges ("orderEvent") so the
    # history reads identically across all three SDKs; the handler stays snake_case.
    @workflow.signal(name="orderEvent")
    def order_event(self, payload: str) -> None:
        self._events.append(payload)
