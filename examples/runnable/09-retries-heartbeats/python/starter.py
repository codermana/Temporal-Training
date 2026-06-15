"""Start one ProcessingWorkflow and print its result (standalone client).

Run the Worker first (worker.py) in another terminal, then:

    python starter.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client

from processing import TASK_QUEUE, ProcessingWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

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
