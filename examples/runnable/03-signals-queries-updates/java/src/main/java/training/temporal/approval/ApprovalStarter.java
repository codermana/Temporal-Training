package training.temporal.approval;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Standalone client: starts one waiting {@code approval-demo} Workflow, then
 * prints the CLI commands you can use to drive it. Run {@link ApprovalWorker}
 * first in another terminal so there is something polling the Task Queue.
 *
 * <pre>
 *   temporal workflow query  --workflow-id approval-demo --type currentState
 *   temporal workflow update execute --workflow-id approval-demo --name changeNote \
 *       --input '"expedite before close of business"'
 *   temporal workflow signal --workflow-id approval-demo --name approve \
 *       --input '"manager@example.com"'
 * </pre>
 *
 * The Workflow blocks in {@code Workflow.await} until an approve/reject Signal
 * arrives, so the query and update run against a live execution.
 */
public class ApprovalStarter {
  private static final String TASK_QUEUE = "approval";
  private static final String WORKFLOW_ID = "approval-demo";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    ApprovalWorkflow workflow =
        client.newWorkflowStub(
            ApprovalWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId(WORKFLOW_ID)
                .build());

    try {
      WorkflowClient.start(workflow::run, "PO-1001");
      System.out.println("Started Workflow '" + WORKFLOW_ID + "', waiting for a decision.");
    } catch (WorkflowExecutionAlreadyStarted alreadyRunning) {
      System.out.println("Workflow '" + WORKFLOW_ID + "' is already running; reusing it.");
    }

    System.out.println("Try, in another terminal:");
    System.out.println(
        "  temporal workflow query  --workflow-id approval-demo --type currentState");
    System.out.println(
        "  temporal workflow update execute --workflow-id approval-demo --name changeNote"
            + " --input '\"expedite before close of business\"'");
    System.out.println(
        "  temporal workflow signal --workflow-id approval-demo --name approve"
            + " --input '\"manager@example.com\"'");
    System.exit(0);
  }
}
