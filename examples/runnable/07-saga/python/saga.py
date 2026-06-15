"""Order saga: authorize payment -> reserve inventory -> ship, with compensation.

The Python equivalent of the Java OrderSagaWorkflow lab. Python has no built-in
Saga helper, so we manage compensations by hand with a list used as a stack: each
forward step appends its undo, and on failure we run them in reverse.
"""

from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

TASK_QUEUE = "orders"


# --- Forward steps ---


@activity.defn
async def authorize_payment(order_id: str) -> str:
    return f"payment-{order_id}"


@activity.defn
async def reserve_inventory(order_id: str) -> str:
    return f"reservation-{order_id}"


@activity.defn
async def ship(order_id: str) -> None:
    # Fail on demand so you can trigger compensation from the CLI.
    if "fail" in order_id.lower():
        raise RuntimeError("shipping label service failed")


# --- Compensating steps (should be idempotent: they may be retried) ---


@activity.defn
async def cancel_payment(payment_id: str) -> None:
    activity.logger.info("cancelled %s", payment_id)


@activity.defn
async def restore_inventory(reservation_id: str) -> None:
    activity.logger.info("restored %s", reservation_id)


@activity.defn
async def send_failure_notification(order_id: str, reason: str) -> None:
    activity.logger.info("order %s failed: %s", order_id, reason)


@workflow.defn
class OrderSagaWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> str:
        # Bound the retries so a permanent failure exhausts quickly and the except
        # block runs the compensations. Without maximum_attempts the default policy
        # retries forever and the saga never reaches compensation.
        opts = dict(
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(milliseconds=500), maximum_attempts=3
            ),
        )

        # Each entry is (activity_fn, arg) to run if we have to unwind.
        compensations: list = []
        try:
            payment_id = await workflow.execute_activity(authorize_payment, order_id, **opts)
            compensations.append((cancel_payment, payment_id))

            reservation_id = await workflow.execute_activity(reserve_inventory, order_id, **opts)
            compensations.append((restore_inventory, reservation_id))

            await workflow.execute_activity(ship, order_id, **opts)
            return "COMPLETED"
        except Exception as failure:
            # Unwind in reverse: most-recent step backwards.
            for compensate, arg in reversed(compensations):
                await workflow.execute_activity(compensate, arg, **opts)
            await workflow.execute_activity(
                send_failure_notification, args=[order_id, str(failure)], **opts
            )
            return "COMPENSATED"
