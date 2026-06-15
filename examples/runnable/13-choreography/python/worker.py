import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from choreography import (
    TASK_QUEUE,
    OrderProcessWorkflow,
    release_inventory,
    request_shipment,
    reserve_local_inventory,
)


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderProcessWorkflow],
        activities=[reserve_local_inventory, request_shipment, release_inventory],
    )
    print(f"Worker started on task queue '{TASK_QUEUE}'. Ctrl-C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

