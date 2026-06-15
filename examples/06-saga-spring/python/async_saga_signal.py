from dataclasses import dataclass

from temporalio.client import Client

from saga_compensation import OrderSagaWorkflow

# Fire-and-forget saga submission. start_workflow with start_signal guarantees the
# workflow exists before the signal is delivered — the Python equivalent of Java's
# signalWithStart. Without it, the signal would race the start RPC.


@dataclass
class OrderRequest:
    order_id: str


async def submit_order(client: Client, request: OrderRequest) -> None:
    await client.start_workflow(
        OrderSagaWorkflow.process,
        request.order_id,
        id=f"order-{request.order_id}",
        task_queue="orders",
        # start_signal delivers a signal atomically with start (signal-with-start).
        start_signal="submit",
        start_signal_args=[request],
    )
