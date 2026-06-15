from temporalio.client import Client
from temporalio.contrib.opentelemetry import TracingInterceptor
from temporalio.worker import Worker

# OpenTelemetry span propagation. A single TracingInterceptor is registered on
# both the Client and the Worker (in Java these are two separate client/worker
# interceptors). Spans started on the client flow through Workflow and Activity
# executions via headers, so one trace spans the whole run. Requires the
# `temporalio[opentelemetry]` extra and a configured global tracer provider.


async def create_client() -> Client:
    return await Client.connect(
        "127.0.0.1:7233",
        interceptors=[TracingInterceptor()],
    )


def create_worker(client: Client) -> Worker:
    # The same interceptor type on the Worker continues the trace into Activities.
    return Worker(
        client,
        task_queue="traced",
        workflows=[],
        activities=[],
        interceptors=[TracingInterceptor()],
    )
