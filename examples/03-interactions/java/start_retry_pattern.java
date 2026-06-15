import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;

// If the client times out or loses connectivity while starting a Workflow, the
// result is ambiguous: Temporal may have committed WorkflowExecutionStarted but
// the response never reached the caller. Retry with the same Workflow ID and
// treat "already started" as success.
class StartRetryPattern {
  void startOrder(WorkflowClient client, String orderId) {
    String workflowId = "order-" + orderId;

    OrderWorkflow workflow =
        client.newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue("orders")
                .build());

    try {
      WorkflowClient.start(workflow::run, orderId);
      System.out.println("Started " + workflowId);
    } catch (WorkflowExecutionAlreadyStarted alreadyStarted) {
      System.out.println("Already started; treating retry as success: " + workflowId);
    }
  }
}

