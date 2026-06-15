from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# `build_report` is a module-level @activity.defn function.


@workflow.defn
class ActivityOptionsExamples:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            build_report,
            # One attempt must finish inside this window.
            start_to_close_timeout=timedelta(minutes=5),
            # The whole retry series must finish inside this window.
            schedule_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
                maximum_interval=timedelta(minutes=1),
                maximum_attempts=6,
            ),
        )
