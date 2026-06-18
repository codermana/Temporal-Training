package training.temporal.tracing;

/**
 * Each Activity sleeps briefly so its span has a visible, distinct duration in
 * the Jaeger timeline. The work itself is fake.
 */
public class OrderActivitiesImpl implements OrderActivities {
  @Override
  public void validateOrder(String orderId) {
    sleep(200);
  }

  @Override
  public String chargePayment(String orderId) {
    sleep(500);
    return "pay-" + Math.abs(orderId.hashCode() % 100000);
  }

  @Override
  public String shipOrder(String orderId) {
    sleep(300);
    return "trk-" + Math.abs(orderId.hashCode() % 100000);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
