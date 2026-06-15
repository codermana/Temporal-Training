"""Run the worker and kick off one OrderPricingWorkflow, mirroring PricingWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from pricing import TASK_QUEUE, OrderPricingWorkflow, PricingActivities


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    activities = PricingActivities()
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderPricingWorkflow],
        activities=[activities.price],
    ):
        total = await client.execute_workflow(
            OrderPricingWorkflow.total,
            ["book", "lamp", "desk"],
            id="order-pricing-demo",
            task_queue=TASK_QUEUE,
        )
        print(f"Total price: {total}")


if __name__ == "__main__":
    asyncio.run(main())
