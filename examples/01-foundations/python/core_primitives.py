from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


# DAG -> Workflow: durable orchestration state and decisions.
@workflow.defn
class LoanWorkflow:
    @workflow.run
    async def apply(self, application_id: str) -> str: ...


# Operator -> Activity: unreliable work with retries, timeouts, and side effects.
# In Python, Activities are plain functions marked with @activity.defn.
@activity.defn
async def pull_credit_score(application_id: str) -> int: ...


@activity.defn
async def notify_applicant(application_id: str, decision: str) -> None: ...


# Executor -> Worker: a long-running process polling a Task Queue.
async def start(client: Client) -> None:
    worker = Worker(
        client,
        task_queue="loan-decisions",
        workflows=[LoanWorkflow],
        activities=[pull_credit_score, notify_applicant],
    )
    await worker.run()
