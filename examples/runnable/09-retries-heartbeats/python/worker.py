"""Run the Worker and kick off one ProcessingWorkflow, mirroring RetriesWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from processing import TASK_QUEUE, FlakyActivities, ProcessingWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    activities = FlakyActivities()
    async with Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ProcessingWorkflow],
        activities=[activities.charge_card, activities.export_large_report],
    ):
        result = await client.execute_workflow(
            ProcessingWorkflow.process,
            "order-42",
            id="retries-heartbeats-demo",
            task_queue=TASK_QUEUE,
        )
        print(f"Result: {result}")
        print(
            "Open the Web UI and look for two ActivityTaskFailed events before charge_card succeeds,"
        )
        print("and the heartbeats recorded on export_large_report.")


if __name__ == "__main__":
    asyncio.run(main())
