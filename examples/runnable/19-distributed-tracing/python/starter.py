"""Start one OrderWorkflow and print its result (standalone client).

The TracingInterceptor opens the root span and propagates its context to the
server, so the Worker's spans nest under it. Run the Worker first (worker.py).

    uv run starter.py [order_id]   # needs Temporal on :7233 and Jaeger OTLP on :4318
"""

import asyncio
import sys

from temporalio.client import Client

from pipeline import TASK_QUEUE, OrderWorkflow
from telemetry import init_tracer, tracing_interceptor


async def main() -> None:
    provider = init_tracer("temporal-client-python")

    client = await Client.connect(
        "127.0.0.1:7233", interceptors=[tracing_interceptor()]
    )

    order_id = sys.argv[1] if len(sys.argv) > 1 else "A-1001"

    result = await client.execute_workflow(
        OrderWorkflow.process,
        order_id,
        id=f"order-{order_id}",
        task_queue=TASK_QUEUE,
    )
    print(result)
    print(
        "Open the trace in Jaeger: http://localhost:16686 "
        "(service temporal-client-python)"
    )

    # Flush the root span before the process exits.
    provider.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
