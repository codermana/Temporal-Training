from temporalio.client import Client

# `OrderWorkflow` is the workflow class with @workflow.run run(order_id) and a
# @workflow.signal order_event(payload). `record` is a kafka-python ConsumerRecord.


# start_workflow(..., start_signal=...) is the Python equivalent of Java's
# signalWithStart: it starts the Workflow on the first event for a key, and the
# attached signal is delivered to that same execution. A re-running id reuse
# policy means later events signal the existing run rather than erroring.
async def on_kafka_record(client: Client, record) -> None:
    order_id = record.key.decode()
    await client.start_workflow(
        OrderWorkflow.run,
        order_id,
        id=f"order-{order_id}",
        task_queue="orders",
        start_signal="order_event",
        start_signal_args=[record.value.decode()],
    )
