"""Keep Workflow history small: pass S3 references (URIs), not file contents,
between steps. The Activities do the heavy I/O; the Workflow only sequences URIs.

The Python port of s3_reference_payload.java. boto3 is imported lazily so this
module py_compiles without the AWS SDK installed.
"""

from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow


@dataclass
class TransformRequest:
    input_s3_uri: str
    output_prefix: str


@dataclass
class TransformResult:
    output_s3_uri: str
    row_count: int


def _s3_client():
    import boto3  # lazy: AWS SDK may be absent offline

    return boto3.client("s3")


@activity.defn
async def transform(input_s3_uri: str, output_prefix: str) -> str:
    # Read from input_s3_uri, write the transformed object under output_prefix,
    # and return the *new* URI - never the bytes.
    s3 = _s3_client()  # noqa: F841  (illustrative; AWS call elided)
    return f"{output_prefix}/transformed.parquet"


@activity.defn
async def count_rows(output_s3_uri: str) -> int:
    s3 = _s3_client()  # noqa: F841
    return 0  # illustrative


@workflow.defn
class TransformWorkflow:
    @workflow.run
    async def run(self, request: TransformRequest) -> TransformResult:
        # Pass S3 references, not file contents, to keep history lean.
        output_uri = await workflow.execute_activity(
            transform,
            args=[request.input_s3_uri, request.output_prefix],
            start_to_close_timeout=timedelta(minutes=5),
        )
        row_count = await workflow.execute_activity(
            count_rows,
            output_uri,
            start_to_close_timeout=timedelta(minutes=2),
        )
        return TransformResult(output_s3_uri=output_uri, row_count=row_count)
