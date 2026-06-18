"""Run the Distributed Tracing Worker (standalone) with OpenTelemetry tracing.

The TracingInterceptor continues the trace started on the client into the
workflow and each activity, so the spans land in one Jaeger trace. Polls the
'distributed-tracing' Task Queue forever; kick off a run with starter.py.

    uv run worker.py    # needs Temporal on 127.0.0.1:7233 and Jaeger OTLP on :4318
"""

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from pipeline import (
    TASK_QUEUE,
    OrderWorkflow,
    charge_payment,
    ship_order,
    validate_order,
)
from telemetry import init_tracer, tracing_interceptor


async def main() -> None:
    provider = init_tracer("temporal-worker-python")

    client = await Client.connect(
        "127.0.0.1:7233", interceptors=[tracing_interceptor()]
    )

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[OrderWorkflow],
        activities=[validate_order, charge_payment, ship_order],
    )
    print(
        f"Tracing Worker started on task queue '{TASK_QUEUE}'. "
        "Traces -> Jaeger UI at http://localhost:16686. Ctrl-C to stop."
    )
    try:
        await worker.run()
    finally:
        provider.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
