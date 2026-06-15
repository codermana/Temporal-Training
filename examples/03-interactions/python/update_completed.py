from temporalio import workflow
from temporalio.client import Client


# An Update is a strongly-typed RPC into a running Workflow that can return a
# result. The @<update>.validator runs first, on the server's accepted path:
# raising rejects the Update before it ever mutates state (like @UpdateValidatorMethod).
@workflow.defn
class CartWorkflow:
    def __init__(self) -> None:
        self._items: dict[str, int] = {}

    @workflow.run
    async def checkout(self, cart_id: str) -> str:
        await workflow.wait_condition(lambda: False)  # waits for a real signal in practice
        return cart_id

    @workflow.update
    def add_item(self, sku: str, quantity: int) -> int:
        self._items[sku] = self._items.get(sku, 0) + quantity
        return sum(self._items.values())

    @add_item.validator
    def validate_add_item(self, sku: str, quantity: int) -> None:
        if quantity <= 0:
            raise ValueError("quantity must be positive")


async def update_cart(client: Client, workflow_id: str) -> int:
    handle = client.get_workflow_handle(workflow_id)
    # COMPLETED waits for the handler to finish and returns its result.
    item_count = await handle.execute_update(
        CartWorkflow.add_item, args=["book", 2]
    )
    return item_count
