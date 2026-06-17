"""Run the three Worker pools this demo routes across.

In production each pool is its own deployment; here one process hosts all three
so the routing is easy to watch. Each Worker registers only the SUBSET of work
routed to its Task Queue.
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from routing import (
    TASK_QUEUE_MEDIA,
    TASK_QUEUE_ORDERS,
    TASK_QUEUE_PAYMENTS,
    MediaActivities,
    OrderWorkflow,
    PaymentActivities,
)


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    payments = PaymentActivities()
    media = MediaActivities()

    # Orchestrator pool: the Workflow only, no Activities.
    orders_worker = Worker(
        client, task_queue=TASK_QUEUE_ORDERS, workflows=[OrderWorkflow]
    )
    # Payments pool: ONLY the charge Activity.
    payments_worker = Worker(
        client, task_queue=TASK_QUEUE_PAYMENTS, activities=[payments.charge]
    )
    # Media/GPU pool: ONLY the render Activity.
    media_worker = Worker(
        client, task_queue=TASK_QUEUE_MEDIA, activities=[media.render]
    )

    print(
        "Workers started: orders=[OrderWorkflow], payments=[charge], "
        "media=[render]. Ctrl-C to stop."
    )
    # Run all three pools concurrently in this one process.
    async with orders_worker, payments_worker, media_worker:
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
