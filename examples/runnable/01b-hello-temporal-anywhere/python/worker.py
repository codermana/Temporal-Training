"""Run the env-driven Hello Worker (standalone). Polls the 'hello-anywhere' Task
Queue forever; kick off a Workflow with starter.py in another terminal. The
connection is built from environment variables, so the same Worker targets a
local dev server, a Dockerized cluster (Lab 1.2b), or Temporal Cloud (Lab 1.2c).

    python worker.py
    # local:  (nothing to set — defaults to 127.0.0.1:7233)
    # cloud:  TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... python worker.py
"""

import asyncio

from temporalio.worker import Worker

from connections import from_env
from greeting import TASK_QUEUE, GreetingWorkflow, compose_greeting


async def main() -> None:
    client = await from_env()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    )
    print(f"Worker started on task queue '{TASK_QUEUE}'. Ctrl-C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
