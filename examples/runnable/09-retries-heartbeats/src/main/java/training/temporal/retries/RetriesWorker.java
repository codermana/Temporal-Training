package training.temporal.retries;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class RetriesWorker {
  private static final String TASK_QUEUE = "retries-heartbeats";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(ProcessingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new FlakyActivitiesImpl());
    factory.start();

    ProcessingWorkflow workflow =
        client.newWorkflowStub(
            ProcessingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("retries-heartbeats-demo")
                .build());

    WorkflowClient.start(workflow::process, "order-42");
    String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
    System.out.println("Result: " + result);
    System.out.println(
        "Open the Web UI and look for two ActivityTaskFailed events before chargeCard succeeds,");
    System.out.println("and the ActivityTaskStarted heartbeats on exportLargeReport.");

    factory.shutdown();
  }
}
