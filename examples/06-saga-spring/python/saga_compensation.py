from datetime import timedelta

from temporalio import activity, workflow

# Python has no built-in Saga helper, so we manage compensations by hand: a list
# used as a stack. Each forward step pushes its undo onto the stack; on failure we
# pop and run them in reverse — the same semantics as Java's io.temporal.workflow.Saga.


# --- Forward and compensating Activities (module-level @activity.defn) ---


@activity.defn
async def authorize_payment(order_id: str) -> str:
    return f"payment-{order_id}"


@activity.defn
async def reserve_inventory(order_id: str) -> str:
    return f"reservation-{order_id}"


@activity.defn
async def ship(order_id: str) -> None:
    if "fail" in order_id.lower():
        raise RuntimeError("shipping label service failed")


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
        # Each entry is (activity_fn, arg) to run if we have to unwind.
        compensations: list = []
        try:
            payment_id = await workflow.execute_activity(
                authorize_payment, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
            compensations.append((cancel_payment, payment_id))

            reservation_id = await workflow.execute_activity(
                reserve_inventory, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
            compensations.append((restore_inventory, reservation_id))

            await workflow.execute_activity(
                ship, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
            return "COMPLETED"
        except Exception as failure:
            # Unwind in reverse: most-recent step backwards.
            for compensate, arg in reversed(compensations):
                await workflow.execute_activity(
                    compensate, arg, start_to_close_timeout=timedelta(seconds=30)
                )
            await workflow.execute_activity(
                send_failure_notification,
                args=[order_id, str(failure)],
                start_to_close_timeout=timedelta(seconds=30),
            )
            return "COMPENSATED"
