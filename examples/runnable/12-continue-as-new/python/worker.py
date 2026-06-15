"""Run the worker and kick off one CounterWorkflow, mirroring ContinueAsNewWorker.java.

    python worker.py        # needs a Temporal dev server on 127.0.0.1:7233

get_result transparently follows the chain of continue-as-new runs to the final
result, so the client sees a single Workflow ID with multiple chained Runs.
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from counter import TASK_QUEUE, CounterWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    async with Worker(client, task_queue=TASK_QUEUE, workflows=[CounterWorkflow]):
        result = await client.execute_workflow(
            CounterWorkflow.count,
            0,
            id="continue-as-new-demo",
            task_queue=TASK_QUEUE,
        )
        print(f"Result: {result}")
        print(
            "In the Web UI, the single Workflow ID shows multiple Runs chained by ContinueAsNew."
        )


if __name__ == "__main__":
    asyncio.run(main())
