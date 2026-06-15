from datetime import timedelta
from unittest.mock import AsyncMock

from temporalio import activity, workflow
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

# The Python analogue of the JUnit 5 + Mockito Activity-mocking test. Instead of
# a TestWorkflowExtension we use WorkflowEnvironment.start_time_skipping(), and
# instead of a Mockito mock we register a fake @activity.defn with the same name
# as the real one (here via an AsyncMock wrapped in a named activity).


@activity.defn
async def lookup_email(user_id: str) -> str:
    raise NotImplementedError  # real impl does I/O; tests register a fake


@workflow.defn
class ReminderWorkflow:
    @workflow.run
    async def remind(self, user_id: str) -> str:
        email = await workflow.execute_activity(
            lookup_email, user_id, start_to_close_timeout=timedelta(seconds=10)
        )
        return f"sent to {email}"


async def test_completes_with_mocked_activity() -> None:
    fake = AsyncMock(return_value="u1@example.com")

    @activity.defn(name="lookup_email")
    async def lookup_email_mock(user_id: str) -> str:
        return await fake(user_id)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="test-reminder",
            workflows=[ReminderWorkflow],
            activities=[lookup_email_mock],
        ):
            result = await env.client.execute_workflow(
                ReminderWorkflow.remind,
                "u1",
                id="reminder-test",
                task_queue="test-reminder",
            )
    assert result == "sent to u1@example.com"
    fake.assert_awaited_once_with("u1")
