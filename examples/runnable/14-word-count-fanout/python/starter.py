"""Start one WordCountWorkflow and print its total."""

import asyncio

from temporalio.client import Client

from word_count import SAMPLE_CHUNKS, TASK_QUEUE, WordCountWorkflow


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    total = await client.execute_workflow(
        WordCountWorkflow.count,
        SAMPLE_CHUNKS,
        id="word-count-demo",
        task_queue=TASK_QUEUE,
    )
    print(f"Total words: {total}")


if __name__ == "__main__":
    asyncio.run(main())
