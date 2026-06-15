// Real-world fan-out: a nightly billing run spawns one child Workflow per tenant.
// Each tenant gets its own Workflow ID (independently queryable / retryable), and
// ParentClosePolicy.ABANDON lets long tenant jobs outlive the coordinator. The
// parent starts every child before joining, so all tenants bill in parallel.
class TenantBillingFanout {

  Map<String, String> run(List<String> tenantIds) {
    Map<String, Promise<String>> futures = new LinkedHashMap<>();

    for (String tenantId : tenantIds) {
      TenantBillingWorkflow child =
          Workflow.newChildWorkflowStub(
              TenantBillingWorkflow.class,
              ChildWorkflowOptions.newBuilder()
                  .setWorkflowId("billing-" + tenantId)
                  .setTaskQueue("billing")
                  .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                  .setWorkflowExecutionTimeout(Duration.ofHours(1))
                  .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
                  .build());
      // Async.function starts the child without blocking; collect the Promises
      // first so every tenant runs concurrently.
      futures.put(tenantId, Async.function(child::run, tenantId));
    }

    Map<String, String> results = new LinkedHashMap<>();
    futures.forEach((tenantId, promise) -> results.put(tenantId, promise.get()));
    return results;
  }
}
