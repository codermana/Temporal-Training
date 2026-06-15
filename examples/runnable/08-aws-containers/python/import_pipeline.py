"""The import pipeline (validate -> transform -> load) as a Workflow + Activities.

The Python equivalent of the Java ImportWorkflow lab. Activities pass S3 *URIs*
forward, never file bytes, so Workflow history stays small. The bodies here fake
the work with sleeps + URI rewriting; in the lab they'd call LocalStack S3.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

TASK_QUEUE = "transform"


@activity.defn
async def validate(input_s3_uri: str) -> str:
    await asyncio.sleep(0.25)
    activity.heartbeat("validated")
    return input_s3_uri.replace("/incoming/", "/validated/")


@activity.defn
async def transform(validated_s3_uri: str) -> str:
    await asyncio.sleep(1)
    activity.heartbeat("transformed")
    return validated_s3_uri.replace("/validated/", "/transformed/")


@activity.defn
async def load(transformed_s3_uri: str) -> int:
    await asyncio.sleep(0.5)
    return abs(hash(transformed_s3_uri)) % 10_000


@workflow.defn
class ImportWorkflow:
    @workflow.run
    async def run(self, input_s3_uri: str) -> str:
        opts = dict(
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1), maximum_attempts=3
            ),
        )
        validated_uri = await workflow.execute_activity(validate, input_s3_uri, **opts)
        transformed_uri = await workflow.execute_activity(transform, validated_uri, **opts)
        row_count = await workflow.execute_activity(load, transformed_uri, **opts)
        return f"{transformed_uri}?rows={row_count}"
