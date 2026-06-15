package training.temporal.parallel;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;

/**
 * Standalone client: starts one OrderPricingWorkflow and prints its result. Run
 * {@link PricingWorker} first in another terminal so there is something polling
 * the Task Queue.
 */
public class PricingStarter {
  private static final String TASK_QUEUE = "pricing";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    OrderPricingWorkflow workflow =
        client.newWorkflowStub(
            OrderPricingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("order-pricing-demo")
                .setTaskQueue(TASK_QUEUE)
                .build());

    int total = workflow.total(List.of("book", "lamp", "desk"));
    System.out.println("Total price: " + total);
    System.exit(0);
  }
}
