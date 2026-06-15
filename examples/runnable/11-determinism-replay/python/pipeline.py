"""Data pipeline workflow + a 'harmless looking' reordered refactor.

The Python equivalent of the Java determinism-replay lab. The original Workflow
extracts THEN loads; the reordered version loads THEN extracts. Both share the
same Workflow type name, so a history recorded from the original replays against
the reordered code — and the different command order trips a non-determinism
error. That regression is exactly what replay testing catches.
"""

from datetime import timedelta

from temporalio import activity, workflow

TASK_QUEUE = "replay-demo"
WORKFLOW_NAME = "DataPipelineWorkflow"


@activity.defn
async def extract() -> str:
    return "rows:100"


@activity.defn
async def load(data: str) -> str:
    return f"loaded {data}"


@workflow.defn(name=WORKFLOW_NAME)
class DataPipelineWorkflow:
    """The original, shipped version: extract THEN load."""

    @workflow.run
    async def run(self) -> str:
        opts = dict(start_to_close_timeout=timedelta(seconds=10))
        extracted = await workflow.execute_activity(extract, **opts)
        return await workflow.execute_activity(load, extracted, **opts)


@workflow.defn(name=WORKFLOW_NAME)
class ReorderedPipelineWorkflow:
    """The breaking refactor: load THEN extract — same type name, new command order."""

    @workflow.run
    async def run(self) -> str:
        opts = dict(start_to_close_timeout=timedelta(seconds=10))
        loaded = await workflow.execute_activity(load, "rows:0", **opts)
        extracted = await workflow.execute_activity(extract, **opts)
        return f"{loaded} / {extracted}"
