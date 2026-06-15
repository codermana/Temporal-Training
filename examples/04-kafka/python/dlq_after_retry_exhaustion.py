from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

# `validate` and `publish_to_dlq` are module-level @activity.defn functions.


@workflow.defn
class DlqRoutingWorkflow:
    # Temporal owns the retry series. Once `validate` exhausts its maximum_attempts,
    # the await raises an ActivityError here — that's our signal to route the
    # poison message to a Kafka DLQ instead of crashing the Workflow.
    @workflow.run
    async def process(self, order_id: str) -> None:
        try:
            await workflow.execute_activity(
                validate,
                order_id,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5),
            )
        except ActivityError as exhausted:
            await workflow.execute_activity(
                publish_to_dlq,
                args=[order_id, str(exhausted)],
                start_to_close_timeout=timedelta(seconds=10),
            )
