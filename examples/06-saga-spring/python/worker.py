"""Idiomatic Python wiring for the order saga.

Spring Boot autoconfig is a Java-only concept. The Python analogue is a plain
module that builds the client + worker (optionally driven by a FastAPI/Flask
lifespan in a web app). There is no DI container — you construct the client and
register workflows/activities explicitly, which is the equivalent of
spring_temporal_config.java's bean wiring.
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from saga_compensation import (
    OrderSagaWorkflow,
    authorize_payment,
    cancel_payment,
    reserve_inventory,
    restore_inventory,
    send_failure_notification,
    ship,
)

TASK_QUEUE = "orders"


async def make_worker(client: Client) -> Worker:
    # Equivalent of WorkerFactory.newWorker("orders") + registrations.
    return Worker(
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
    )


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")
    worker = await make_worker(client)
    async with worker:
        # In a FastAPI app you'd start the worker in the lifespan and serve forever.
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
