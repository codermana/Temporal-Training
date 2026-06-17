package training.temporal.routing;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Starts one OrderWorkflow on the {@code orders} Task Queue and prints the
 * summary. Run {@link RoutingWorker} first so the three pools are polling.
 *
 * <p>Watch the Worker terminal: the {@code [payments pool]} and {@code [media
 * pool]} log lines come from two different Workers, even though neither
 * registered the full set of Activities.
 */
public class RoutingStarter {
  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    OrderWorkflow workflow =
        client.newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("order-routing-demo")
                .setTaskQueue(TaskQueues.ORDERS)
                .build());

    String summary = workflow.process("order-1001");
    System.out.println("Workflow result: " + summary);
    System.exit(0);
  }
}
