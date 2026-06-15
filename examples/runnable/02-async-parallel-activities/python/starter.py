"""Start one OrderPricingWorkflow and print its result (standalone client).

Run the Worker first (worker.py) in another terminal, then:

    python starter.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client

from pricing import TASK_QUEUE, OrderPricingWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    total = await client.execute_workflow(
        OrderPricingWorkflow.total,
        ["book", "lamp", "desk"],
        id="order-pricing-demo",
        task_queue=TASK_QUEUE,
    )
    print(f"Total price: {total}")


if __name__ == "__main__":
    asyncio.run(main())
