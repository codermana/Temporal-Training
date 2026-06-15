"""Activities are at-least-once: a retried or timed-out Activity can run its side
effect twice. For non-idempotent externals (charge a card, POST to a partner),
guard with a DynamoDB conditional write keyed by the Activity's idempotency key
(e.g. the Workflow ID + step). The conditional put is the dedupe; the partner
call only runs on the first writer.

boto3 is imported lazily so this module py_compiles without the AWS SDK.
"""

from temporalio import activity


def _ddb_table(name: str):
    import boto3  # lazy: AWS SDK may be absent offline

    return boto3.resource("dynamodb").Table(name)


def _call_payment_partner(amount_cents: int) -> str:
    raise NotImplementedError  # the non-idempotent side effect


@activity.defn
async def charge_once(idempotency_key: str, amount_cents: int) -> str:
    from botocore.exceptions import ClientError

    table = _ddb_table("idempotency")
    try:
        # Conditional put: succeeds only if this key was never seen before.
        table.put_item(
            Item={"pk": idempotency_key, "status": "IN_PROGRESS"},
            ConditionExpression="attribute_not_exists(pk)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        # A previous attempt already claimed this key — return the stored result.
        item = table.get_item(Key={"pk": idempotency_key}).get("Item", {})
        return item.get("result", "")

    result = _call_payment_partner(amount_cents)

    table.update_item(
        Key={"pk": idempotency_key},
        UpdateExpression="SET #s = :done, #r = :result",
        ExpressionAttributeNames={"#s": "status", "#r": "result"},
        ExpressionAttributeValues={":done": "DONE", ":result": result},
    )
    return result
