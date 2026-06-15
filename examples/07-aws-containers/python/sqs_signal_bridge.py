"""An "EventBridge rule -> Lambda -> StartExecution" trigger becomes a long-poll
SQS consumer that starts or signals Workflows. The bridge is plain glue code (it
runs outside any Workflow): receive a message, translate it to a start_workflow
(start_signal makes it signal-with-start), delete it. Idempotent on the Workflow
ID, so an at-least-once SQS redelivery just re-signals the same Workflow.

boto3 is imported lazily so this module py_compiles without the AWS SDK.
"""

import json

from temporalio.client import Client


def _sqs_client():
    import boto3  # lazy: AWS SDK may be absent offline

    return boto3.client("sqs")


async def pump(client: Client, queue_url: str) -> None:
    sqs = _sqs_client()
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # long poll, not a hot spin
        )
        for m in resp.get("Messages", []):
            event = json.loads(m["Body"])
            s3_uri = event["s3Uri"]

            # start_signal: starts the Workflow if absent, signals it if running.
            await client.start_workflow(
                "ImportWorkflow",
                s3_uri,
                id=f"import-{event['bucket']}-{event['key']}",
                task_queue="transform",
                start_signal="file_arrived",
                start_signal_args=[s3_uri],
            )

            # Delete only after the signal is durable in Temporal — at-least-once.
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
