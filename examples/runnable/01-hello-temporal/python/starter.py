"""Start one GreetingWorkflow and print its result (standalone client).

Run the Worker first (worker.py) in another terminal, then:

    python starter.py        # needs a Temporal dev server on 127.0.0.1:7233
"""

import asyncio

from temporalio.client import Client

from greeting import TASK_QUEUE, GreetingWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    result = await client.execute_workflow(
        GreetingWorkflow.greet,
        "Ada",
        id="hello-temporal-demo",
        task_queue=TASK_QUEUE,
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
