"""Run the Worker and execute one GreetingWorkflow, mirroring HelloWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from greeting import TASK_QUEUE, GreetingWorkflow, compose_greeting


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    ):
        result = await client.execute_workflow(
            GreetingWorkflow.greet,
            "Ada",
            id="hello-temporal-demo",
            task_queue=TASK_QUEUE,
        )
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
