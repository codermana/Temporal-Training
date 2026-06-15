from dataclasses import dataclass
from datetime import timedelta
from typing import List

from temporalio import activity, workflow

TASK_QUEUE = "choreography"
WORKFLOW_ID = "order-choreo-1001"


@dataclass
class DomainEvent:
    order_id: str
    type: str
    payload: str


@activity.defn
async def reserve_local_inventory(order_id: str) -> None:
    print(f"reserved local inventory for {order_id}")


@activity.defn
async def request_shipment(order_id: str) -> None:
    print(f"requested shipment for {order_id}")


@activity.defn
async def release_inventory(order_id: str, reason: str) -> None:
    print(f"released inventory for {order_id}: {reason}")


@workflow.defn
class OrderProcessWorkflow:
    def __init__(self) -> None:
        self._order_id = ""
        self._status = "WAITING_FOR_ORDER"
        self._events: List[DomainEvent] = []
        self._failure_reason = ""

    @workflow.run
    async def run(self, order_id: str) -> str:
        self._order_id = order_id

        await workflow.wait_condition(lambda: self._has_event("OrderPlaced"))
        self._status = "ORDER_ACCEPTED"
        await workflow.execute_activity(
            reserve_local_inventory,
            order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        await workflow.wait_condition(
            lambda: self._has_event("PaymentCaptured")
            or self._has_event("PaymentFailed")
        )
        if self._has_event("PaymentFailed"):
            self._status = "COMPENSATING_PAYMENT_FAILURE"
            await workflow.execute_activity(
                release_inventory,
                args=[order_id, self._failure_reason],
                start_to_close_timeout=timedelta(seconds=10),
            )
            self._status = "CANCELLED"
            return self.status()

        self._status = "PAID_WAITING_FOR_INVENTORY"
        await workflow.wait_condition(lambda: self._has_event("InventoryReserved"))
        await workflow.execute_activity(
            request_shipment,
            order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )

        self._status = "READY_TO_SHIP"
        return self.status()

    @workflow.signal
    def on_event(self, event: DomainEvent) -> None:
        self._events.append(event)
        if event.type == "PaymentFailed":
            self._failure_reason = event.payload

    @workflow.query
    def status(self) -> str:
        return f"{self._order_id} {self._status} events={len(self._events)}"

    def _has_event(self, event_type: str) -> bool:
        return any(event.type == event_type for event in self._events)

