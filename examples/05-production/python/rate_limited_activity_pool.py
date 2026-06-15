from temporalio.client import Client
from temporalio.worker import Worker

# NEW production snippet: rate-limit a pool of Workers calling a fragile downstream
# (a legacy API, a vendor with a QPS cap). Two complementary knobs:
#   max_activities_per_second           -> per-Worker cap
#   max_task_queue_activities_per_second -> GLOBAL cap across every Worker polling
#                                           this task queue (the server enforces it)
# Use the task-queue-wide limit to protect the dependency no matter how many
# Worker replicas you scale to. This is the Python analogue of Java
# WorkerOptions.setMaxTaskQueueActivitiesPerSecond.


async def start(client: Client) -> None:
    worker = Worker(
        client,
        task_queue="legacy-api-calls",
        workflows=[],
        activities=[],
        # Cap THIS worker to 50 activity starts/sec...
        max_activities_per_second=50,
        # ...and the whole task queue (all replicas combined) to 100/sec, so the
        # vendor never sees more than 100 QPS even at 20 worker replicas.
        max_task_queue_activities_per_second=100,
    )
    await worker.run()
