package training.temporal.saga;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import java.util.concurrent.CountDownLatch;

/**
 * Long-lived Worker for the order Saga. Polls the {@code orders} Task Queue and
 * stays alive so Workflows can be started from the CLI:
 *
 * <pre>
 *   temporal workflow start --task-queue orders \
 *     --type OrderSagaWorkflow --workflow-id order-OK --input '"order-1001"'
 *
 *   temporal workflow start --task-queue orders \
 *     --type OrderSagaWorkflow --workflow-id order-fail --input '"fail-at-ship"'
 * </pre>
 */
public class SagaWorker {
  private static final String TASK_QUEUE = "orders";

  public static void main(String[] args) throws InterruptedException {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(OrderSagaWorkflowImpl.class);
    worker.registerActivitiesImplementations(new OrderActivitiesImpl());

    Runtime.getRuntime().addShutdownHook(new Thread(factory::shutdown));

    factory.start();
    System.out.println("Saga Worker polling task queue '" + TASK_QUEUE + "'. Ctrl+C to stop.");
    System.out.println("Start a Workflow with, e.g.:");
    System.out.println(
        "  temporal workflow start --task-queue orders --type OrderSagaWorkflow"
            + " --workflow-id order-OK --input '\"order-1001\"'");

    new CountDownLatch(1).await();
  }
}
