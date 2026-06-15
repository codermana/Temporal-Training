package training.temporal.child;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Standalone Worker: registers the parent + child Workflows and polls the
 * child-workflows Task Queue. Start a run from another terminal with {@link
 * ChildStarter}.
 */
public class ChildWorker {
  private static final String TASK_QUEUE = "child-workflows";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    // Parent and child run on the same Worker here; in production they can poll different queues.
    worker.registerWorkflowImplementationTypes(BatchWorkflowImpl.class, ItemWorkflowImpl.class);

    factory.start();
    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}
