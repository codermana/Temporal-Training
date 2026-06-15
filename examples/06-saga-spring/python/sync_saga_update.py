from dataclasses import dataclass

from temporalio.client import Client, WithStartWorkflowOperation
from temporalio.common import WorkflowIDConflictPolicy

from saga_compensation import OrderSagaWorkflow

# Sync request/response over a saga. execute_update_with_start_workflow creates the
# workflow if it does not exist and applies the update in one round trip — the
# Python equivalent of Java's startUpdateWithStart. The caller waits on the result
# without a separate start RPC.


@dataclass
class OrderRequest:
    order_id: str


async def submit_order(client: Client, request: OrderRequest) -> str:
    start_op = WithStartWorkflowOperation(
        OrderSagaWorkflow.process,
        request.order_id,
        id=f"order-{request.order_id}",
        task_queue="orders",
        id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
    )

    return await client.execute_update_with_start_workflow(
        "submit",
        request,
        start_workflow_operation=start_op,
    )
