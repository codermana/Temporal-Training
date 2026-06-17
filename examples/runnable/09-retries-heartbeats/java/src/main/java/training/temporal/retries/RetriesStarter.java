package training.temporal.retries;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Standalone client: starts one ProcessingWorkflow and prints its result. Run
 * {@link RetriesWorker} first in another terminal so there is something polling
 * the Task Queue.
 */
public class RetriesStarter {
  private static final String TASK_QUEUE = "retries-heartbeats";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    ProcessingWorkflow workflow =
        client.newWorkflowStub(
            ProcessingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("retries-heartbeats-demo")
                .setTaskQueue(TASK_QUEUE)
                .build());

    String result = workflow.process("order-42");
    System.out.println("Result: " + result);
    System.out.println(
        "Note: automatic retries are NOT separate history events. A completed run collapses");
    System.out.println(
        "chargeCard's failed attempts into one ActivityTaskStarted carrying attempt=3 +");
    System.out.println(
        "lastFailure. To watch the live attempt counter, open the Web UI's Pending Activities");
    System.out.println("tab while the run is mid-retry, and watch the heartbeats on exportLargeReport.");
    System.exit(0);
  }
}
