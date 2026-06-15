package training.temporal.child;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;

/**
 * Standalone client: starts one BatchWorkflow (the parent) and prints its
 * result. Run {@link ChildWorker} first in another terminal so there is
 * something polling the Task Queue.
 */
public class ChildStarter {
  private static final String TASK_QUEUE = "child-workflows";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    BatchWorkflow workflow =
        client.newWorkflowStub(
            BatchWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("batch-parent-demo")
                .setTaskQueue(TASK_QUEUE)
                .build());

    String result = workflow.run(List.of("A", "B", "C"));
    System.out.println("Parent result:\n" + result);
    System.out.println(
        "In the Web UI you'll see one parent execution plus child executions item-A / item-B / item-C.");
    System.exit(0);
  }
}
