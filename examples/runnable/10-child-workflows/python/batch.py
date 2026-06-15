"""Parent/child Workflows. The Python equivalent of the Java child-workflows lab.

The parent starts one child per item - each with its own stable Workflow ID, so
it is separately queryable / signalable / cancelable - then waits for all of them.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow

TASK_QUEUE = "child-workflows"


@workflow.defn
class ItemWorkflow:
    """A child Workflow - its own Workflow ID, its own history, separately addressable."""

    @workflow.run
    async def process_item(self, item: str) -> str:
        # A durable sleep so each child is visibly "running" in the Web UI for a moment.
        await workflow.sleep(timedelta(milliseconds=500))
        return f"processed[{item}] in child {workflow.info().workflow_id}"


@workflow.defn
class BatchWorkflow:
    @workflow.run
    async def run(self, items: list[str]) -> str:
        # Start every child before awaiting any → they run in parallel. Each child
        # gets a stable, independent Workflow ID.
        handles = [
            await workflow.start_child_workflow(
                ItemWorkflow.process_item,
                item,
                id=f"item-{item}",
            )
            for item in items
        ]
        results = await asyncio.gather(*handles)
        return "\n".join(results)
