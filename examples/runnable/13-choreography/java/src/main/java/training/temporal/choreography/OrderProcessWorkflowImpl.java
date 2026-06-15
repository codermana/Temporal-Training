package training.temporal.choreography;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class OrderProcessWorkflowImpl implements OrderProcessWorkflow {
  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(10))
              .build());

  private final List<DomainEvent> events = new ArrayList<>();
  private String orderId = "";
  private String status = "WAITING_FOR_ORDER";
  private String failureReason = "";

  @Override
  public String run(String orderId) {
    this.orderId = orderId;

    Workflow.await(() -> hasEvent("OrderPlaced"));
    status = "ORDER_ACCEPTED";
    activities.reserveLocalInventory(orderId);

    Workflow.await(() -> hasEvent("PaymentCaptured") || hasEvent("PaymentFailed"));
    if (hasEvent("PaymentFailed")) {
      status = "COMPENSATING_PAYMENT_FAILURE";
      activities.releaseInventory(orderId, failureReason);
      status = "CANCELLED";
      return status();
    }

    status = "PAID_WAITING_FOR_INVENTORY";
    Workflow.await(() -> hasEvent("InventoryReserved"));
    activities.requestShipment(orderId);

    status = "READY_TO_SHIP";
    return status();
  }

  @Override
  public void onEvent(DomainEvent event) {
    events.add(event);
    if ("PaymentFailed".equals(event.type())) {
      failureReason = event.payload();
    }
  }

  @Override
  public String status() {
    return orderId + " " + status + " events=" + events.size();
  }

  private boolean hasEvent(String eventType) {
    return events.stream().anyMatch(event -> eventType.equals(event.type()));
  }
}

