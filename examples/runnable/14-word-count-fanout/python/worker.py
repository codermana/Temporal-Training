"""Run the word-count Worker. Polls the 'word-count' Task Queue forever."""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from word_count import TASK_QUEUE, WordCountActivities, WordCountWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    activities = WordCountActivities()
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[WordCountWorkflow],
        activities=[activities.count_words],
    )
    print(f"Worker started on task queue '{TASK_QUEUE}'. Ctrl-C to stop.")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
