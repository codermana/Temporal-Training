package training.temporal.routing;

public class MediaActivitiesImpl implements MediaActivities {
  @Override
  public String render(String orderId) {
    // The log line proves which pool executed this Activity.
    System.out.println("[media pool] rendering receipt for order " + orderId);
    return "s3://receipts/" + orderId + ".pdf";
  }
}
