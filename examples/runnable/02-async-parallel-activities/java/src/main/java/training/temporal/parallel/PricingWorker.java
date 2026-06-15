package training.temporal.parallel;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Standalone Worker: registers the Workflow + Activities and polls the pricing
 * Task Queue. Start a run from another terminal with {@link PricingStarter}.
 */
public class PricingWorker {
  private static final String TASK_QUEUE = "pricing";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(OrderPricingWorkflowImpl.class);
    worker.registerActivitiesImplementations(new PricingActivitiesImpl());

    factory.start();
    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}
