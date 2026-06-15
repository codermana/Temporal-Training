"""Run the ProcessingWorkflow Worker (standalone). Polls the 'retries-heartbeats'
Task Queue forever; kick off a Workflow with starter.py in another terminal.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from processing import TASK_QUEUE, FlakyActivities, ProcessingWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    activities = FlakyActivities()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ProcessingWorkflow],
        activities=[activities.charge_card, activities.export_large_report],
    )
    print(f"Worker started on task queue '{TASK_QUEUE}'. Ctrl-C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
