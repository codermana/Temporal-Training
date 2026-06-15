"""Start one BatchWorkflow (the parent) and print its result (standalone client).

Run the Worker first (worker.py) in another terminal, then:

    python starter.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client

from batch import TASK_QUEUE, BatchWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

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
