"""Reminder workflow under test: sleep a day, then return a message.

The Python equivalent of the Java ReminderWorkflow testing lab. The point is that
a one-day sleep completes in milliseconds under the time-skipping test
environment, so the test runs fast and hermetically — no server, no real wait.
"""

from datetime import timedelta

from temporalio import activity, workflow

TASK_QUEUE = "test-reminder"


@activity.defn
async def lookup_email(user_id: str) -> str:
    # Real impl would hit a user service; tests register a fake in its place.
    raise NotImplementedError


@workflow.defn
class ReminderWorkflow:
    @workflow.run
    async def remind_after_one_day(self, message: str) -> str:
        await workflow.sleep(timedelta(days=1))  # skipped by the test env
        return f"Reminder: {message}"


@workflow.defn
class EmailReminderWorkflow:
    @workflow.run
    async def remind(self, user_id: str) -> str:
        email = await workflow.execute_activity(
            lookup_email, user_id, start_to_close_timeout=timedelta(seconds=10)
        )
        return f"sent to {email}"
