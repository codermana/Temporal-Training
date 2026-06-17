package training.temporal.routing;

public class PaymentActivitiesImpl implements PaymentActivities {
  @Override
  public String charge(String orderId) {
    // The log line proves which pool executed this Activity.
    System.out.println("[payments pool] charging order " + orderId);
    return "charged $42.00";
  }
}
