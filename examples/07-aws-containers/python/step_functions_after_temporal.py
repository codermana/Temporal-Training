"""The Step Functions state machine (validate -> transform -> load -> notify),
rewritten as one Temporal Workflow. The JSON state graph becomes straight-line
Python; the Catch(NotifyFailure) becomes a try/except.

The Python port of step_functions_after_temporal.java. The Activities here are
declared elsewhere (e.g. s3_reference_payload); this file is the orchestration.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    # Activity functions live in their own module; import them for the stub calls.
    from import_activities import load, notify, transform, validate


@workflow.defn
class ImportWorkflow:
    @workflow.run
    async def run(self, input_s3_uri: str) -> None:
        opts = dict(
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=3
            ),
        )
        try:
            clean_uri = await workflow.execute_activity(validate, input_s3_uri, **opts)
            transformed_uri = await workflow.execute_activity(transform, clean_uri, **opts)
            load_result = await workflow.execute_activity(load, transformed_uri, **opts)
            await workflow.execute_activity(notify, load_result, **opts)
        except Exception:
            # The Step Functions Catch(NotifyFailure) branch.
            await workflow.execute_activity(notify, "import failed", **opts)
            raise
