import asyncio
from datetime import timedelta

from temporalio import workflow

# `send` is a module-level @activity.defn function (a notification send).


@workflow.defn
class BoundedFanoutWorkflow:
    # Fan out 10k notifications, but a downstream provider only tolerates ~20
    # in-flight calls. asyncio.Semaphore is replay-safe inside a temporalio
    # Workflow (the event loop is deterministic), so it's the idiomatic cap on
    # concurrency while keeping the pipeline full.
    @workflow.run
    async def send_all(self, user_ids: list[str], max_in_flight: int) -> int:
        limiter = asyncio.Semaphore(max_in_flight)

        async def send_one(uid: str) -> None:
            async with limiter:  # acquire a slot, release on completion
                await workflow.execute_activity(
                    send, uid, start_to_close_timeout=timedelta(minutes=1)
                )

        await asyncio.gather(*(send_one(uid) for uid in user_ids))
        return len(user_ids)
