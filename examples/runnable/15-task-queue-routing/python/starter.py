"""Start one OrderWorkflow and print its summary.

Run worker.py first so the three pools are polling. Watch the Worker terminal:
the ``[payments pool]`` and ``[media pool]`` lines come from two different
Workers, even though neither registered the full set of Activities.
"""

import asyncio

from temporalio.client import Client

from routing import TASK_QUEUE_ORDERS, OrderWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    summary = await client.execute_workflow(
        OrderWorkflow.process,
        "order-1001",
        id="order-routing-demo",
        task_queue=TASK_QUEUE_ORDERS,
    )
    print(f"Workflow result: {summary}")


if __name__ == "__main__":
    asyncio.run(main())
