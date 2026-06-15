package training.temporal.choreography;

public class OrderActivitiesImpl implements OrderActivities {
  @Override
  public void reserveLocalInventory(String orderId) {
    System.out.println("reserved local inventory for " + orderId);
  }

  @Override
  public void requestShipment(String orderId) {
    System.out.println("requested shipment for " + orderId);
  }

  @Override
  public void releaseInventory(String orderId, String reason) {
    System.out.println("released inventory for " + orderId + ": " + reason);
  }
}

