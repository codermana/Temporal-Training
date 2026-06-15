"""Run the Worker and kick off one BatchWorkflow, mirroring ChildWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from batch import TASK_QUEUE, BatchWorkflow, ItemWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    # Parent and child run on the same Worker here; in production they can poll
    # different queues.
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[BatchWorkflow, ItemWorkflow],
    ):
        result = await client.execute_workflow(
            BatchWorkflow.run,
            ["A", "B", "C"],
            id="batch-parent-demo",
            task_queue=TASK_QUEUE,
        )
        print("Parent result:\n" + result)
        print(
            "In the Web UI you'll see one parent execution plus child executions"
            " item-A / item-B / item-C."
        )


if __name__ == "__main__":
    asyncio.run(main())
