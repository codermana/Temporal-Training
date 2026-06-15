from temporalio.client import Client
from temporalio.worker import (
    FixedSizeSlotSupplier,
    ResourceBasedSlotConfig,
    ResourceBasedSlotSupplier,
    ResourceBasedTunerConfig,
    Worker,
    WorkerTuner,
)


async def start(client: Client) -> None:
    # A composite tuner mixes slot strategies on one Worker: fixed slots for
    # workflow tasks (cheap, bursty) and resource-based slots for activities (the
    # heavy work). This mirrors Java's CompositeTuner.
    resource_config = ResourceBasedTunerConfig(
        target_memory_usage=0.75, target_cpu_usage=0.80
    )

    tuner = WorkerTuner.create_composite(
        workflow_supplier=FixedSizeSlotSupplier(20),  # workflow task slots
        activity_supplier=ResourceBasedSlotSupplier(  # activity slots
            ResourceBasedSlotConfig(minimum_slots=1, maximum_slots=200),
            resource_config,
        ),
        local_activity_supplier=FixedSizeSlotSupplier(20),  # local activity slots
        nexus_supplier=FixedSizeSlotSupplier(20),
    )

    worker = Worker(
        client,
        task_queue="mixed",
        workflows=[],
        activities=[],
        tuner=tuner,
    )
    await worker.run()
