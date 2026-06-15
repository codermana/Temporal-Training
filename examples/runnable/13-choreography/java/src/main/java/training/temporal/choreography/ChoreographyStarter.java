package training.temporal.choreography;

import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;

public class ChoreographyStarter {
  public static void main(String[] args) throws InterruptedException {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    List<DomainEvent> stream =
        List.of(
            new DomainEvent("choreo-1001", "OrderPlaced", "cart=3 items"),
            new DomainEvent("choreo-1001", "PaymentCaptured", "paymentId=pay-777"),
            new DomainEvent("choreo-1001", "InventoryReserved", "reservation=inv-42"));

    for (DomainEvent event : stream) {
      signalWithStart(client, event);
      System.out.println("accepted event " + event.type() + " for " + event.orderId());
      Thread.sleep(750);
    }

    OrderProcessWorkflow workflow =
        client.newWorkflowStub(OrderProcessWorkflow.class, workflowId("choreo-1001"));
    System.out.println("final status: " + workflow.status());
    System.exit(0);
  }

  private static void signalWithStart(WorkflowClient client, DomainEvent event) {
    OrderProcessWorkflow workflow =
        client.newWorkflowStub(
            OrderProcessWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId(event.orderId()))
                .setTaskQueue(ChoreographyWorker.TASK_QUEUE)
                .build());

    BatchRequest batch = client.newSignalWithStartRequest();
    batch.add(workflow::run, event.orderId());
    batch.add(workflow::onEvent, event);
    client.signalWithStart(batch);
  }

  private static String workflowId(String orderId) {
    return "order-" + orderId;
  }
}

