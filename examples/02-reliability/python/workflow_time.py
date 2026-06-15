from datetime import timedelta

from temporalio import workflow


@workflow.defn
class TimeWorkflow:
    @workflow.run
    async def run(self) -> None:
        # Replay-safe timer. Records a TimerStarted event and frees the worker
        # thread instead of really sleeping. Never use asyncio.sleep / time.sleep.
        await workflow.sleep(timedelta(hours=6))

        # Replay-safe time source. Never call datetime.now() in a Workflow.
        deadline = workflow.now() + timedelta(days=1)
