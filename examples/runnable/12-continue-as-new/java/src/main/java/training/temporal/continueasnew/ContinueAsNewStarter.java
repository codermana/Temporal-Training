package training.temporal.continueasnew;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Standalone client: starts one CounterWorkflow and prints its result. Run
 * {@link ContinueAsNewWorker} first in another terminal so there is something
 * polling the Task Queue.
 */
public class ContinueAsNewStarter {
  private static final String TASK_QUEUE = "continue-as-new";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    CounterWorkflow workflow =
        client.newWorkflowStub(
            CounterWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("continue-as-new-demo")
                .build());

    // getResult transparently follows the chain of continue-as-new runs to the final result.
    WorkflowClient.start(workflow::count, 0);
    String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
    System.out.println("Result: " + result);
    System.out.println(
        "In the Web UI, the single Workflow ID shows multiple Runs chained by ContinueAsNew.");
    System.exit(0);
  }
}
