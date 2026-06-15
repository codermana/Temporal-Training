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
        "Open the Web UI and look for two ActivityTaskFailed events before chargeCard succeeds,");
    System.out.println("and the ActivityTaskStarted heartbeats on exportLargeReport.");
    System.exit(0);
  }
}
