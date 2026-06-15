package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Standalone Worker: registers the Workflow + Activities and polls the Task Queue. Start a run from
 * another terminal with {@link HelloStarter}.
 *
 * <p>The connection is built by {@link Connections#fromEnv()} instead of {@code
 * WorkflowServiceStubs.newLocalServiceStubs()}. That single env-driven helper lets the same Worker
 * target a local dev server, a Dockerized cluster (Lab 1.2b), or Temporal Cloud (Lab 1.2c) — only
 * environment variables change, never this code.
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
    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}
