package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * The Lab 1.2 Worker with one change: the connection is built by {@link Connections#fromEnv()}
 * instead of {@code WorkflowServiceStubs.newLocalServiceStubs()}. That single env-driven helper
 * lets the same Worker target a local dev server, a Dockerized cluster (Lab 1.2b), or Temporal
 * Cloud (Lab 1.2c) — only environment variables change, never this code.
 */
public class HelloWorker {
  private static final String TASK_QUEUE = "hello-temporal";

  public static void main(String[] args) {
    WorkflowClient client = Connections.fromEnv();

    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new GreetingActivitiesImpl());
    factory.start();

    GreetingWorkflow workflow =
        client.newWorkflowStub(
            GreetingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("hello-temporal-demo")
                .build());

    // Sleep here => Stopped cluster

    WorkflowClient.start(workflow::greet, "Ada");
    String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
    System.out.println(result);

    factory.shutdown();
  }
}
