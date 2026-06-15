from dataclasses import dataclass
from datetime import timedelta
from typing import List

from temporalio import workflow
from temporalio.client import Client


@dataclass
class DomainEvent:
    order_id: str
    type: str
    payload: str


@workflow.defn
class OrderProcessWorkflow:
    def __init__(self) -> None:
        self.buffered_events: List[DomainEvent] = []
        self.shipment_failed = False

    @workflow.run
    async def run(self, order_id: str) -> None:
        await workflow.wait_condition(lambda: self._has_event("OrderPlaced"))
        await workflow.execute_activity(
            "reserve_inventory",
            order_id,
            start_to_close_timeout=timedelta(seconds=30),
        )

        await workflow.wait_condition(
            lambda: self._has_event("PaymentCaptured") or self.shipment_failed
        )
        if self.shipment_failed:
            await workflow.execute_activity(
                "release_inventory",
                order_id,
                start_to_close_timeout=timedelta(seconds=30),
            )
            return

        await workflow.execute_activity(
            "request_shipment",
            order_id,
            start_to_close_timeout=timedelta(seconds=30),
        )

    @workflow.signal
    async def on_event(self, event: DomainEvent) -> None:
        self.buffered_events.append(event)
        if event.type == "ShipmentFailed":
            self.shipment_failed = True

    def _has_event(self, event_type: str) -> bool:
        return any(event.type == event_type for event in self.buffered_events)


async def on_domain_event(client: Client, event: DomainEvent) -> None:
    # Choreography boundary: Kafka carries facts between services. This handler
    # only admits the event into Temporal; the Workflow owns order-local state.
    await client.start_workflow(
        OrderProcessWorkflow.run,
        event.order_id,
        id=f"order-{event.order_id}",
        task_queue="orders",
        start_signal="on_event",
        start_signal_args=[event],
    )
