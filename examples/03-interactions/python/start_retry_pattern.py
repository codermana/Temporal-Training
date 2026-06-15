from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError


# If the client times out or loses connectivity while starting a Workflow, the
# result is ambiguous: Temporal may have committed WorkflowExecutionStarted but
# the response never reached the caller. Retry with the same Workflow ID and
# treat "already started" as success.
async def start_order(client: Client, order_id: str) -> None:
    workflow_id = f"order-{order_id}"

    try:
        await client.start_workflow(
            OrderWorkflow.run,
            order_id,
            id=workflow_id,
            task_queue="orders",
        )
        print(f"Started {workflow_id}")
    except WorkflowAlreadyStartedError:
        print(f"Already started; treating retry as success: {workflow_id}")

