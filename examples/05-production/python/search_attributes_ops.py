from datetime import timedelta

from temporalio import workflow
from temporalio.common import SearchAttributeKey

# NEW production snippet: emit typed Search Attributes from inside a Workflow so
# ops dashboards / `temporal workflow list` can filter and group live executions
# (e.g. "all orders in REFUNDING for tenant acme"). Register the custom keys once
# per namespace first:
#   temporal operator search-attribute create --name OrderStage --type Keyword
#   temporal operator search-attribute create --name Tenant     --type Keyword

ORDER_STAGE = SearchAttributeKey.for_keyword("OrderStage")
TENANT = SearchAttributeKey.for_keyword("Tenant")


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str, tenant: str) -> str:
        # Set at start so the order shows up immediately in ops queries.
        workflow.upsert_search_attributes(
            [TENANT.value_set(tenant), ORDER_STAGE.value_set("RECEIVED")]
        )

        await workflow.execute_activity(
            charge, order_id, start_to_close_timeout=timedelta(seconds=30)
        )
        # Update the stage as the order moves through the pipeline.
        workflow.upsert_search_attributes([ORDER_STAGE.value_set("CHARGED")])

        await workflow.execute_activity(
            ship, order_id, start_to_close_timeout=timedelta(minutes=5)
        )
        workflow.upsert_search_attributes([ORDER_STAGE.value_set("SHIPPED")])
        return "done"
