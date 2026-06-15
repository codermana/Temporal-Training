@WorkflowInterface
interface OrderProcessWorkflow {
  @WorkflowMethod
  void run(String orderId);

  @SignalMethod
  void onEvent(DomainEvent event);
}

record DomainEvent(String orderId, String type, String payload) {}

class OrderProcessWorkflowImpl implements OrderProcessWorkflow {
  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .build());

  private final List<DomainEvent> bufferedEvents = new ArrayList<>();
  private boolean shipmentFailed;

  @Override
  public void run(String orderId) {
    Workflow.await(() -> hasEvent("OrderPlaced"));
    activities.reserveInventory(orderId);

    Workflow.await(() -> hasEvent("PaymentCaptured") || shipmentFailed);
    if (shipmentFailed) {
      activities.releaseInventory(orderId);
      return;
    }

    activities.requestShipment(orderId);
  }

  @Override
  public void onEvent(DomainEvent event) {
    bufferedEvents.add(event);
    if ("ShipmentFailed".equals(event.type())) {
      shipmentFailed = true;
    }
  }

  private boolean hasEvent(String type) {
    return bufferedEvents.stream().anyMatch(event -> type.equals(event.type()));
  }
}

class DomainEventBridge {
  private final WorkflowClient client;

  DomainEventBridge(WorkflowClient client) {
    this.client = client;
  }

  // Choreography boundary: Kafka carries facts between services. This listener
  // only admits the event into Temporal; the Workflow owns order-local state.
  @KafkaListener(topics = "order-domain-events")
  void onEvent(DomainEvent event) {
    OrderProcessWorkflow workflow =
        client.newWorkflowStub(
            OrderProcessWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("order-" + event.orderId())
                .setTaskQueue("orders")
                .build());

    BatchRequest batch = client.newSignalWithStartRequest();
    batch.add(workflow::run, event.orderId());
    batch.add(workflow::onEvent, event);
    client.signalWithStart(batch);
  }
}
