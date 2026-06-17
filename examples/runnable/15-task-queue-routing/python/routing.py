"""Routing Activities to different Worker pools by Task Queue.

One Workflow (`OrderWorkflow`, on the ``orders`` queue) whose two Activities are
routed to separate pools: ``charge`` runs on the ``payments`` queue, ``render``
on the ``media`` queue. The unit of "who registers what" is the Task Queue, not
the Worker - no single Worker registers the full set.
"""

from datetime import timedelta

from temporalio import activity, workflow

# Each name maps to one Worker pool that registers only its subset of work.
TASK_QUEUE_ORDERS = "orders"
TASK_QUEUE_PAYMENTS = "payments"
TASK_QUEUE_MEDIA = "media"


class PaymentActivities:
    @activity.defn
    async def charge(self, order_id: str) -> str:
        # The log line proves which pool executed this Activity.
        print(f"[payments pool] charging order {order_id}")
        return "charged $42.00"


class MediaActivities:
    @activity.defn
    async def render(self, order_id: str) -> str:
        print(f"[media pool] rendering receipt for order {order_id}")
        return f"s3://receipts/{order_id}.pdf"


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> str:
        # Each call names the Task Queue whose pool registers that Activity.
        charged = await workflow.execute_activity_method(
            PaymentActivities.charge,
            order_id,
            task_queue=TASK_QUEUE_PAYMENTS,
            start_to_close_timeout=timedelta(seconds=30),
        )
        receipt = await workflow.execute_activity_method(
            MediaActivities.render,
            order_id,
            task_queue=TASK_QUEUE_MEDIA,
            start_to_close_timeout=timedelta(minutes=5),
        )
        return f"{order_id}: {charged} / {receipt}"
