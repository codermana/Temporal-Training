from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

# `OrderSagaWorkflow` is a @workflow.defn class defined elsewhere.


async def replays_production_history() -> None:
    # The Python analogue of WorkflowReplayer.replayWorkflowExecutionFromResource.
    # Load a recorded history and replay it against the current code; replay_workflow
    # raises if the new code produces a divergent command stream (a non-determinism).
    history_json = Path("histories/order-1001.json").read_text()
    replayer = Replayer(workflows=[OrderSagaWorkflow])  # noqa: F821
    await replayer.replay_workflow(WorkflowHistory.from_json("order-1001", history_json))
