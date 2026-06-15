package training.temporal.choreography;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class ChoreographyWorker {
  static final String TASK_QUEUE = "choreography";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(OrderProcessWorkflowImpl.class);
    worker.registerActivitiesImplementations(new OrderActivitiesImpl());

    factory.start();
    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}

