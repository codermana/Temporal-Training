"""The final "notify" step of an import — in AWS an SNS publish wrapped in a
Lambda (or an EventBridge fan-out) — becomes an Activity that publishes to an
SNS topic. Publishing is I/O, so it lives in an Activity, never in Workflow
code. Subscribers (email, SQS, Lambda) stay decoupled: the publisher only knows
the topic ARN, not who's listening.

SNS delivery is at-least-once, so a retried Activity may publish the same
notification twice. Make the message self-identifying — include the workflowId +
runId — so consumers can dedup, and on a FIFO topic pass a message-deduplication
id (the workflowId) so SNS itself collapses duplicates.

The Python port of sns_publish_activity.java. boto3 is imported lazily so this
module py_compiles without the AWS SDK; the SNS client points at LocalStack to
mirror the Glue/S3 client config in the earlier labs.
"""

import json

from temporalio import activity


def _sns_client():
    import boto3  # lazy: AWS SDK may be absent offline

    return boto3.client(
        "sns",
        endpoint_url="http://127.0.0.1:4566",  # LocalStack
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@activity.defn
async def publish_notification(
    topic_arn: str, workflow_id: str, row_count: int, output_s3_uri: str
) -> str:
    sns = _sns_client()

    # The workflowId + runId travel in the body so subscribers can dedup an
    # at-least-once redelivery; this is a small notification, not a data bus —
    # pass the output URI, never the rows themselves.
    message = json.dumps(
        {
            "workflowId": workflow_id,
            "runId": activity.info().workflow_run_id,
            "rowCount": row_count,
            "outputS3Uri": output_s3_uri,
        }
    )

    resp = sns.publish(
        TopicArn=topic_arn,
        Subject="import-complete",
        Message=message,
        # FIFO topic: the workflowId is the dedup id, so SNS collapses a retried
        # publish into one delivery. Ignored on a standard topic.
        MessageDeduplicationId=workflow_id,
        MessageGroupId="imports",
    )
    return resp["MessageId"]  # surfaces in the UI as the Activity result
