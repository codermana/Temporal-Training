import concurrent.futures

from temporalio.client import Client
from temporalio.worker import Worker

# Python has no JVM virtual threads (Java's virtual_threads.java). The concurrency
# model is different: async @activity.defn functions run cooperatively on the
# worker's asyncio event loop, so thousands of I/O-bound activities can be in
# flight on a single thread without a thread-per-activity cost — the same payoff
# virtual threads give the JVM. Concurrency is capped by max_concurrent_activities.
#
# Blocking (sync def) activities, by contrast, DO need a real worker thread each;
# give the Worker an explicit thread (or process) pool executor for those.


async def start_async_worker(client: Client) -> None:
    worker = Worker(
        client,
        task_queue="high-concurrency-activities",
        workflows=[],
        activities=[],  # async @activity.defn functions: cheap, event-loop bound
        max_concurrent_activities=1000,
    )
    await worker.run()


async def start_sync_worker(client: Client) -> None:
    # Sync activities each occupy a pool thread, so size the pool deliberately.
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
        worker = Worker(
            client,
            task_queue="blocking-activities",
            workflows=[],
            activities=[],  # blocking (def) @activity.defn functions
            activity_executor=pool,
            max_concurrent_activities=100,
        )
        await worker.run()
