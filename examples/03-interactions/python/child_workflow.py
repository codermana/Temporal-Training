import asyncio

from temporalio import workflow


# A parent fans out to child Workflows the same way it fans out to Activities:
# start every child before awaiting any, then asyncio.gather. start_child_workflow
# returns a handle whose .result() resolves when the child completes; pass a
# task_queue so each child can run on its own Worker pool.
@workflow.defn
class ParentOrderWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> None:
        fraud = await workflow.start_child_workflow(
            FraudWorkflow.check, order_id, task_queue="fraud"
        )
        shipping = await workflow.start_child_workflow(
            ShippingWorkflow.plan, order_id, task_queue="shipping"
        )

        await asyncio.gather(fraud, shipping)
