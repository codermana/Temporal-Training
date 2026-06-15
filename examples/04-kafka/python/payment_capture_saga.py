from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# `capture`, `publish_settlement`, `refund` are module-level @activity.defn functions.


@workflow.defn
class PaymentCaptureWorkflow:
    # Driven by a `payment-authorized` Kafka event (bridged in via a start signal).
    # Capture the funds, then publish a settlement event. If publishing fails after
    # the money moved, run a compensating refund so the system never ends in a
    # "captured but never reported" state — a Kafka-fed saga.
    @workflow.run
    async def run(self, payment_id: str, amount_cents: int) -> None:
        opts = dict(
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=4),
        )
        capture_ref = await workflow.execute_activity(
            capture, args=[payment_id, amount_cents], **opts
        )
        try:
            await workflow.execute_activity(
                publish_settlement,
                args=["payment-settlements", payment_id, capture_ref],
                **opts,
            )
        except ActivityError:
            await workflow.execute_activity(refund, args=[payment_id, capture_ref], **opts)
            raise ApplicationError(
                f"settlement publish failed, refunded {payment_id}",
                type="SettlementPublishFailed",
            )
