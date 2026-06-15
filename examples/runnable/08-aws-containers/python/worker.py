"""Run the import Worker, mirroring WorkerMain.java. Env-driven so the same image
serves any Task Queue.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233

Env: TEMPORAL_ADDRESS (default 127.0.0.1:7233), TEMPORAL_NAMESPACE (default),
TASK_QUEUE (transform).
"""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from import_pipeline import ImportWorkflow, load, transform, validate


async def main() -> None:
    target = os.getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    task_queue = os.getenv("TASK_QUEUE", "transform")

    client = await Client.connect(target, namespace=namespace)
    print(f"Worker started. target={target} namespace={namespace} taskQueue={task_queue}")

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[ImportWorkflow],
        activities=[validate, transform, load],
    ):
        # Block forever; SIGTERM (docker stop / pod delete) drains gracefully.
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
