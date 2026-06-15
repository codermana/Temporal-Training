"""Workflow + Activity for the env-driven Hello lab (same as 01-hello-temporal)."""

from datetime import timedelta

from temporalio import activity, workflow

TASK_QUEUE = "hello-anywhere"


@activity.defn
async def compose_greeting(name: str) -> str:
    return f"Hello, {name} from a Temporal Activity"


@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def greet(self, name: str) -> str:
        return await workflow.execute_activity(
            compose_greeting, name, start_to_close_timeout=timedelta(seconds=10)
        )
