"""Env-driven Hello Temporal (Lab 1.2b Docker / 1.2c Cloud), Python version.

    python worker.py
    # local:  (nothing to set — defaults to 127.0.0.1:7233)
    # cloud:  TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... python worker.py
"""

import asyncio
import os

from temporalio.worker import Worker

from connections import from_env
from greeting import TASK_QUEUE, GreetingWorkflow, compose_greeting


async def main() -> None:
    client = await from_env()

    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    ):
        result = await client.execute_workflow(
            GreetingWorkflow.greet,
            os.environ.get("GREET_NAME", "Ada"),
            id="hello-anywhere-demo",
            task_queue=TASK_QUEUE,
        )
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
