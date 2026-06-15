"""Replay determinism test, mirroring ReplayDeterminismTest.java.

Records a real history in-process, then replays it against the original code
(clean) and the reordered refactor (fails). Run it:

    pip install -r requirements.txt
    pytest -q
"""

import uuid

import pytest
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from pipeline import (
    TASK_QUEUE,
    DataPipelineWorkflow,
    ReorderedPipelineWorkflow,
    extract,
    load,
)


async def _record_history() -> WorkflowHistory:
    """Run the shipped workflow once in-process and capture its event history."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[DataPipelineWorkflow],
            activities=[extract, load],
        ):
            handle = await env.client.start_workflow(
                DataPipelineWorkflow.run,
                id=f"pipeline-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )
            await handle.result()
            return await handle.fetch_history()


@pytest.mark.asyncio
async def test_shipped_code_replays_clean() -> None:
    history = await _record_history()
    replayer = Replayer(workflows=[DataPipelineWorkflow])
    # No exception == the recorded history still matches the current code.
    await replayer.replay_workflow(history)


@pytest.mark.asyncio
async def test_reordered_code_breaks_replay() -> None:
    history = await _record_history()
    # The reordered refactor produces different commands -> replay raises. CI fails here.
    replayer = Replayer(workflows=[ReorderedPipelineWorkflow])
    with pytest.raises(Exception):
        await replayer.replay_workflow(history)
