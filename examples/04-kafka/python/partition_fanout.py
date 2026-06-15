import asyncio
from datetime import timedelta

from temporalio import workflow

# `process_range` is a module-level @activity.defn function taking a (start, end)
# partition range and returning an int count.


@workflow.defn
class PartitionRangeWorkflow:
    # Fan one Activity out per partition range, then asyncio.gather to join. Keep
    # the fan-out capped (a handful of ranges) for laptops and predictable
    # Activity pressure — the equivalent of Java's Promise.allOf over ranges.
    @workflow.run
    async def process_ranges(self) -> int:
        ranges = [(0, 3), (4, 7), (8, 11)]
        counts = [
            workflow.execute_activity(
                process_range,
                args=[start, end],
                start_to_close_timeout=timedelta(minutes=20),
            )
            for start, end in ranges
        ]
        return sum(await asyncio.gather(*counts))
