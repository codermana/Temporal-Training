// NEW production snippet: emit typed Search Attributes from inside a Workflow so
// ops dashboards / `temporal workflow list` can filter and group live executions
// (e.g. "all orders in REFUNDING for tenant acme"). Register the custom keys once
// per namespace first:
//   temporal operator search-attribute create --name OrderStage --type Keyword
//   temporal operator search-attribute create --name Tenant     --type Keyword
class SearchAttributeWorkflowImpl implements OrderWorkflow {
  private static final SearchAttributeKey<String> ORDER_STAGE =
      SearchAttributeKey.forKeyword("OrderStage");
  private static final SearchAttributeKey<String> TENANT =
      SearchAttributeKey.forKeyword("Tenant");

  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

  @Override
  public String run(String orderId, String tenant) {
    // Set at start so the order shows up immediately in ops queries.
    Workflow.upsertTypedSearchAttributes(
        TENANT.valueSet(tenant), ORDER_STAGE.valueSet("RECEIVED"));

    activities.charge(orderId);
    // Update the stage as the order moves through the pipeline.
    Workflow.upsertTypedSearchAttributes(ORDER_STAGE.valueSet("CHARGED"));

    activities.ship(orderId);
    Workflow.upsertTypedSearchAttributes(ORDER_STAGE.valueSet("SHIPPED"));
    return "done";
  }
}
