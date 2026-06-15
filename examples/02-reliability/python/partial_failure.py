import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError

# `process_with_status` is a module-level @activity.defn function.


@workflow.defn
class PartialFailureWorkflow:
    # gather(return_exceptions=True) lets some branches fail without aborting the
    # whole fan-out: failures arrive as exception objects in the result list, so
    # the Workflow can record a per-partition status instead of crashing.
    @workflow.run
    async def process(self, partitions: list[int]) -> dict[int, str]:
        futures = {
            partition: workflow.execute_activity(
                process_with_status,
                partition,
                start_to_close_timeout=timedelta(minutes=15),
            )
            for partition in partitions
        }
        outcomes = await asyncio.gather(*futures.values(), return_exceptions=True)

        result: dict[int, str] = {}
        for partition, outcome in zip(futures.keys(), outcomes):
            if isinstance(outcome, ActivityError):
                result[partition] = f"FAILED: {outcome}"
            else:
                result[partition] = outcome
        return result
