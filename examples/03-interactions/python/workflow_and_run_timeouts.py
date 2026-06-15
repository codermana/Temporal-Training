from datetime import timedelta

from temporalio.client import Client


# Two distinct lifetimes on the start options:
#   execution_timeout - the whole Workflow, summed across continue-as-new runs.
#   run_timeout       - one individual run before it must finish or continue-as-new.
async def start(client: Client) -> None:
    await client.start_workflow(
        OrdersWorkflow.run,
        "2026-05-27",
        id="orders-2026-05-27",
        task_queue="orders",
        # Maximum lifetime across continue-as-new runs.
        execution_timeout=timedelta(days=7),
        # Maximum lifetime for this individual run.
        run_timeout=timedelta(hours=12),
    )
