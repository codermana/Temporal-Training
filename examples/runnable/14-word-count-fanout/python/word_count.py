"""Word-count workflow: count document chunks in parallel, then sum."""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

TASK_QUEUE = "word-count"

SAMPLE_CHUNKS = [
    "Temporal workflows keep the overall document job durable.",
    "Each activity receives one chunk and counts its words.",
    "The workflow starts every chunk count before waiting for results.",
    "Fan-in sums the partial counts into one final total.",
]


class WordCountActivities:
    @activity.defn
    async def count_words(self, chunk: str) -> int:
        activity.heartbeat("counting chunk")
        return len(chunk.split())


@workflow.defn
class WordCountWorkflow:
    @workflow.run
    async def count(self, chunks: list[str]) -> int:
        # Start one Activity per chunk before awaiting any of them.
        counts = [
            workflow.execute_activity_method(
                WordCountActivities.count_words,
                chunk,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1), maximum_attempts=3
                ),
            )
            for chunk in chunks
        ]
        return sum(await asyncio.gather(*counts))
