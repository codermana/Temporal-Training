import asyncio

from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from choreography import TASK_QUEUE, WORKFLOW_ID, DomainEvent, OrderProcessWorkflow


async def admit_event(client: Client, event: DomainEvent) -> None:
    handle = client.get_workflow_handle(WORKFLOW_ID)
    try:
        await client.start_workflow(
            OrderProcessWorkflow.run,
            event.order_id,
            id=WORKFLOW_ID,
            task_queue=TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        pass

    await handle.signal(OrderProcessWorkflow.on_event, event)


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")
    stream = [
        DomainEvent("choreo-1001", "OrderPlaced", "cart=3 items"),
        DomainEvent("choreo-1001", "PaymentCaptured", "paymentId=pay-777"),
        DomainEvent("choreo-1001", "InventoryReserved", "reservation=inv-42"),
    ]

    for event in stream:
        await admit_event(client, event)
        print(f"accepted event {event.type} for {event.order_id}")
        await asyncio.sleep(0.75)

    handle = client.get_workflow_handle(WORKFLOW_ID)
    print("final status:", await handle.query(OrderProcessWorkflow.status))


if __name__ == "__main__":
    asyncio.run(main())

