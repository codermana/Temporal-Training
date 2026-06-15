"""Pytest suite mirroring ReminderWorkflowTest.java.

Run it (a Temporal test server is auto-downloaded + run in-process, no dev server
needed):

    pip install -r requirements.txt
    pytest -q
"""

import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from reminder import (
    TASK_QUEUE,
    EmailReminderWorkflow,
    ReminderWorkflow,
)


@pytest.mark.asyncio
async def test_skips_workflow_time() -> None:
    # Time-skipping env: the one-day Workflow.sleep returns immediately, so the
    # whole test runs in well under a second.
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ReminderWorkflow],
        ):
            result = await env.client.execute_workflow(
                ReminderWorkflow.remind_after_one_day,
                "ship report",
                id=f"reminder-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )
    assert result == "Reminder: ship report"


@pytest.mark.asyncio
async def test_mocks_the_activity() -> None:
    # The Mockito analogue: register a fake @activity.defn with the same name as
    # the real one, so no real I/O happens in the test.
    @activity.defn(name="lookup_email")
    async def lookup_email_mock(user_id: str) -> str:
        return "u1@example.com"

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[EmailReminderWorkflow],
            activities=[lookup_email_mock],
        ):
            result = await env.client.execute_workflow(
                EmailReminderWorkflow.remind,
                "u1",
                id=f"email-{uuid.uuid4()}",
                task_queue=TASK_QUEUE,
            )
    assert result == "sent to u1@example.com"


@pytest.mark.asyncio
async def test_activity_failure_surfaces() -> None:
    # Negative path: a mocked Activity that always throws makes the Workflow fail.
    @activity.defn(name="lookup_email")
    async def lookup_email_boom(user_id: str) -> str:
        raise RuntimeError("user service down")

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[EmailReminderWorkflow],
            activities=[lookup_email_boom],
            # Don't burn the whole suite retrying a deterministic failure.
        ):
            with pytest.raises(Exception):
                await env.client.execute_workflow(
                    EmailReminderWorkflow.remind,
                    "u1",
                    id=f"email-fail-{uuid.uuid4()}",
                    task_queue=TASK_QUEUE,
                    run_timeout=timedelta(seconds=10),
                )
