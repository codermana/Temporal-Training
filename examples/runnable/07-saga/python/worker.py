"""Long-lived Saga Worker on the "orders" queue, mirroring SagaWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233

Then start a Workflow from the CLI:

    temporal workflow start --task-queue orders --type OrderSagaWorkflow \
      --workflow-id order-OK --input '"order-1001"'

    temporal workflow start --task-queue orders --type OrderSagaWorkflow \
      --workflow-id order-fail --input '"fail-at-ship"'
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from saga import (
    TASK_QUEUE,
    OrderSagaWorkflow,
    authorize_payment,
    cancel_payment,
    reserve_inventory,
    restore_inventory,
    send_failure_notification,
    ship,
)


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderSagaWorkflow],
        activities=[
            authorize_payment,
            reserve_inventory,
            ship,
            cancel_payment,
            restore_inventory,
            send_failure_notification,
        ],
    ):
        print(f"Saga Worker polling task queue '{TASK_QUEUE}'. Ctrl+C to stop.")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
