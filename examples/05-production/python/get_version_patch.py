from datetime import timedelta

from temporalio import workflow

# `reserve` and `charge` are module-level @activity.defn functions.


@workflow.defn
class VersionedWorkflow:
    # workflow.patched / workflow.deprecate_patch are the Python equivalent of
    # Java's Workflow.getVersion. patched(id) returns True for new executions and
    # False when replaying histories recorded before the patch existed.
    @workflow.run
    async def run(self, order_id: str) -> None:
        if workflow.patched("charge-before-reserve"):
            await workflow.execute_activity(
                charge, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
            await workflow.execute_activity(
                reserve, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
        else:
            await workflow.execute_activity(
                reserve, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
            await workflow.execute_activity(
                charge, order_id, start_to_close_timeout=timedelta(seconds=30)
            )
