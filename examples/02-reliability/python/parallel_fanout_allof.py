import asyncio
from datetime import timedelta

from temporalio import workflow

# `process_partition` is a module-level @activity.defn function returning an int.


@workflow.defn
class PartitionFanoutWorkflow:
    # Launch one Activity per partition, then asyncio.gather to wait for all of
    # them. gather preserves order, so summing the results is straightforward.
    @workflow.run
    async def process_partitions(self, partitions: list[int]) -> int:
        counts = [
            workflow.execute_activity(
                process_partition,
                partition,
                start_to_close_timeout=timedelta(minutes=15),
            )
            for partition in partitions
        ]
        return sum(await asyncio.gather(*counts))
