package training.temporal.child;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.util.List;

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

    BatchWorkflow workflow =
        client.newWorkflowStub(
            BatchWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("batch-parent-demo")
                .build());

    WorkflowClient.start(workflow::run, List.of("A", "B", "C"));
    String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
    System.out.println("Parent result:\n" + result);
    System.out.println(
        "In the Web UI you'll see one parent execution plus child executions item-A / item-B / item-C.");

    factory.shutdown();
  }
}
