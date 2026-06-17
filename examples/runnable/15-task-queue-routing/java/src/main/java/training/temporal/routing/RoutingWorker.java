package training.temporal.routing;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Starts the three Worker pools this demo routes across. In production each pool
 * is its own deployment on its own hardware; here one process hosts all three so
 * the routing is easy to watch. Note how each {@code factory.newWorker(queue)}
 * registers only the SUBSET of work routed to its Task Queue:
 *
 * <ul>
 *   <li>{@code orders} pool   -> the Workflow only (no Activities).
 *   <li>{@code payments} pool -> PaymentActivities only.
 *   <li>{@code media} pool    -> MediaActivities only.
 * </ul>
 *
 * Start a run from another terminal with {@link RoutingStarter}.
 */
public class RoutingWorker {
  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    // Orchestrator pool: registers the Workflow, no Activities.
    Worker orders = factory.newWorker(TaskQueues.ORDERS);
    orders.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);

    // Payments pool: registers ONLY PaymentActivities.
    Worker payments = factory.newWorker(TaskQueues.PAYMENTS);
    payments.registerActivitiesImplementations(new PaymentActivitiesImpl());

    // Media/GPU pool: registers ONLY MediaActivities.
    Worker media = factory.newWorker(TaskQueues.MEDIA);
    media.registerActivitiesImplementations(new MediaActivitiesImpl());

    factory.start();
    System.out.println(
        "Workers started: orders=[OrderWorkflow], payments=[PaymentActivities], "
            + "media=[MediaActivities]. Ctrl-C to stop.");
  }
}
