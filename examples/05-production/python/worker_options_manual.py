from temporalio.client import Client
from temporalio.worker import Worker

# Manual Worker sizing. The Python SDK runs activities on an asyncio event loop
# (and a thread/process pool for sync activities) rather than JVM threads, so the
# knobs are concurrency limits rather than thread-pool sizes. These are the direct
# analogue of Java's setMaxConcurrentActivityExecutionSize / WorkflowTask sizes.


async def start(client: Client) -> None:
    worker = Worker(
        client,
        task_queue="io-heavy",
        workflows=[],  # register IoWorkflow here
        activities=[],  # register IoActivities here
        max_concurrent_activities=200,
        max_concurrent_workflow_tasks=20,
    )
    await worker.run()
