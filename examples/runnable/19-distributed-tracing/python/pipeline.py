"""Distributed Tracing — the Python equivalent of the Java OrderWorkflow lab.

A three-step order pipeline. Nothing here is tracing-specific: the spans come
entirely from the TracingInterceptor wired in telemetry.py. Each activity call
becomes a child span under the workflow span.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow

TASK_QUEUE = "distributed-tracing"


# Each activity sleeps briefly so its span has a visible, distinct duration.
@activity.defn
async def validate_order(order_id: str) -> None:
    await asyncio.sleep(0.2)


@activity.defn
async def charge_payment(order_id: str) -> str:
    await asyncio.sleep(0.5)
    return f"pay-{order_id}"


@activity.defn
async def ship_order(order_id: str) -> str:
    await asyncio.sleep(0.3)
    return f"trk-{order_id}"


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> str:
        await workflow.execute_activity(
            validate_order, order_id, start_to_close_timeout=timedelta(seconds=10)
        )
        payment_id = await workflow.execute_activity(
            charge_payment, order_id, start_to_close_timeout=timedelta(seconds=10)
        )
        tracking = await workflow.execute_activity(
            ship_order, order_id, start_to_close_timeout=timedelta(seconds=10)
        )
        return (
            f"order {order_id} complete (payment={payment_id}, tracking={tracking})"
        )
