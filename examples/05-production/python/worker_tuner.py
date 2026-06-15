from temporalio.client import Client
from temporalio.worker import Worker, WorkerTuner


async def start(client: Client) -> None:
    # WorkerTuner.create_resource_based is the Python equivalent of Java's
    # ResourceBasedTuner: the worker hands out slots up to target CPU/memory
    # utilisation instead of a fixed count, so a busy host self-throttles.
    # target_* are fractions (0..1).
    tuner = WorkerTuner.create_resource_based(
        target_memory_usage=0.75,
        target_cpu_usage=0.80,
    )

    worker = Worker(
        client,
        task_queue="payments",
        workflows=[],
        activities=[],
        tuner=tuner,
    )
    await worker.run()
