"""Run a long Glue ETL job from inside an Activity: submit, heartbeat the run id
while polling, and raise a structured ApplicationError on a bad terminal state.

The Python port of glue_activity.java. boto3 is imported lazily so this module
still py_compiles where boto3 is absent; the Temporal-side code is what matters.
"""

import asyncio
from datetime import timedelta

from temporalio import activity
from temporalio.exceptions import ApplicationError

_TERMINAL_BAD = {"FAILED", "TIMEOUT", "STOPPED"}


def _glue_client():
    import boto3  # lazy: AWS SDK may be absent offline

    return boto3.client("glue")


@activity.defn
async def run_glue_job(job_name: str, input_s3_uri: str) -> str:
    glue = _glue_client()
    run_id = glue.start_job_run(
        JobName=job_name, Arguments={"--input": input_s3_uri}
    )["JobRunId"]

    while True:
        activity.heartbeat(run_id)
        state = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"][
            "JobRunState"
        ]

        if state == "SUCCEEDED":
            return run_id
        if state in _TERMINAL_BAD:
            # A typed failure surfaces in the UI as GlueJobFailed, not a stack trace.
            raise ApplicationError(
                f"Glue job {job_name} ended as {state}", type="GlueJobFailed"
            )

        # Back off between polls so we don't hammer the Glue API and burn the
        # throttle limit before the job finishes.
        await asyncio.sleep(timedelta(seconds=15).total_seconds())
