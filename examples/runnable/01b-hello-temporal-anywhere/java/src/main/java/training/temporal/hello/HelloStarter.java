package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;

/**
 * Standalone client: starts one GreetingWorkflow and prints its result. Run {@link HelloWorker}
 * first in another terminal so there is something polling the Task Queue.
 *
 * <p>The connection is built by {@link Connections#fromEnv()} — the same env-driven helper the
 * Worker uses — so the starter targets a local dev server, a Dockerized cluster (Lab 1.2b), or
 * Temporal Cloud (Lab 1.2c) with no code change. Set {@code GREET_NAME} to change the input.
 */
public class HelloStarter {
  private static final String TASK_QUEUE = "hello-temporal";

  public static void main(String[] args) {
    WorkflowClient client = Connections.fromEnv();

    String name = System.getenv().getOrDefault("GREET_NAME", "Ada");

    GreetingWorkflow workflow =
        client.newWorkflowStub(
            GreetingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("hello-anywhere-demo")
                .setTaskQueue(TASK_QUEUE)
                .build());

    String result = workflow.greet(name);
    System.out.println(result);
    System.exit(0);
  }
}
