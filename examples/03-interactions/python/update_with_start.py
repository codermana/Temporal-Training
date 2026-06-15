from temporalio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateStage,
)
from temporalio.common import WorkflowIDConflictPolicy


# Update-with-Start atomically starts the Workflow (if not already running) and
# applies an Update in one round trip - the "add to cart, creating the cart if
# needed" pattern. The start half is a WithStartWorkflowOperation; the conflict
# policy USE_EXISTING reuses a running cart instead of erroring.
async def add_item_or_create_cart(client: Client) -> int:
    start_op = WithStartWorkflowOperation(
        CartWorkflow.checkout,
        "cart-1001",
        id="cart-1001",
        task_queue="carts",
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )

    handle = await client.start_update_with_start_workflow(
        CartWorkflow.add_item,
        args=["lamp", 1],
        start_workflow_operation=start_op,
        wait_for_stage=WorkflowUpdateStage.COMPLETED,
    )

    item_count: int = await handle.result()
    return item_count
