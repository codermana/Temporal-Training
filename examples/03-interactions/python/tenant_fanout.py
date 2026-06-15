import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ParentClosePolicy


# Real-world fan-out: a nightly billing run spawns one child Workflow per tenant.
# Each tenant gets its own Workflow ID (independently queryable / retryable), and
# PARENT_CLOSE_POLICY_ABANDON lets long tenant jobs outlive the coordinator. The
# parent starts every child before awaiting, so all tenants bill in parallel.
@workflow.defn
class TenantBillingFanout:
    @workflow.run
    async def run(self, tenant_ids: list[str]) -> dict[str, str]:
        handles = [
            await workflow.start_child_workflow(
                TenantBillingWorkflow.run,
                tenant_id,
                id=f"billing-{tenant_id}",
                task_queue="billing",
                parent_close_policy=ParentClosePolicy.ABANDON,
                retry_policy=RetryPolicy(maximum_attempts=3),
                execution_timeout=timedelta(hours=1),
            )
            for tenant_id in tenant_ids
        ]
        results = await asyncio.gather(*handles)
        return dict(zip(tenant_ids, results))
