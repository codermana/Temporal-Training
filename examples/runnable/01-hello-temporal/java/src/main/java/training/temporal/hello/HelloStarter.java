package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Standalone client: starts one GreetingWorkflow and prints its result. Run
 * {@link HelloWorker} first in another terminal so there is something polling
 * the Task Queue.
 */
public class HelloStarter {
  private static final String TASK_QUEUE = "hello-temporal";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    GreetingWorkflow workflow =
        client.newWorkflowStub(
            GreetingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("hello-temporal-demo")
                .setTaskQueue(TASK_QUEUE)
                .build());

    String result = workflow.greet("Ada");
    System.out.println(result);
    System.exit(0);
  }
}
