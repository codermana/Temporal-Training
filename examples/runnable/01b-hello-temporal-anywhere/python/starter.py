"""Start one GreetingWorkflow and print its result (standalone client). The
connection is built from the same environment variables as the Worker, so the
starter targets a local dev server, a Dockerized cluster (Lab 1.2b), or Temporal
Cloud (Lab 1.2c). Set GREET_NAME to change the greeting input.

Run the Worker first (worker.py) in another terminal, then:

    python starter.py
    # local:  (nothing to set — defaults to 127.0.0.1:7233)
    # cloud:  TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... python starter.py
"""

import asyncio
import os

from connections import from_env
from greeting import TASK_QUEUE, GreetingWorkflow


async def main() -> None:
    client = await from_env()

    result = await client.execute_workflow(
        GreetingWorkflow.greet,
        os.environ.get("GREET_NAME", "Ada"),
        id="hello-anywhere-demo",
        task_queue=TASK_QUEUE,
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
