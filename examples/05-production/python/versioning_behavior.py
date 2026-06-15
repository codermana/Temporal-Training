from temporalio import workflow
from temporalio.common import VersioningBehavior

# Worker Versioning behavior. Python sets it per-workflow with the
# `versioning_behavior` argument to @workflow.defn (the analogue of Java's
# @WorkflowVersioningBehavior); the enum lives in temporalio.common. PINNED keeps
# existing executions on a compatible Build ID; AUTO_UPGRADE lets long-runners
# move to newer compatible worker code.


@workflow.defn(versioning_behavior=VersioningBehavior.PINNED)
class ShortLivedCheckoutWorkflow:
    @workflow.run
    async def run(self, cart_id: str) -> None:
        # Existing executions stay on compatible workers during rollout.
        ...


@workflow.defn(versioning_behavior=VersioningBehavior.AUTO_UPGRADE)
class SubscriptionLifecycleWorkflow:
    @workflow.run
    async def run(self, subscription_id: str) -> None:
        # Long-running executions can move to newer compatible worker code.
        ...
