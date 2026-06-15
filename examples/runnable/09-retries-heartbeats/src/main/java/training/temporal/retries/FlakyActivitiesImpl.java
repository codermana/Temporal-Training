package training.temporal.retries;

import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.failure.ApplicationFailure;
import java.util.concurrent.atomic.AtomicInteger;

public class FlakyActivitiesImpl implements FlakyActivities {

  // One impl instance is shared across attempts on this Worker, so the counter survives retries.
  private final AtomicInteger chargeAttempts = new AtomicInteger();

  @Override
  public String chargeCard(String orderId) {
    int attempt = chargeAttempts.incrementAndGet();
    System.out.println("chargeCard attempt " + attempt + " for " + orderId);
    if (attempt < 3) {
      // A thrown exception is retried per the Workflow's RetryOptions.
      throw ApplicationFailure.newFailure(
          "payment gateway timeout (attempt " + attempt + ")", "GatewayTimeout");
    }
    return "charged " + orderId + " on attempt " + attempt;
  }

  @Override
  public String exportLargeReport(int pages) {
    ActivityExecutionContext ctx = Activity.getExecutionContext();
    // On retry, resume from the last recorded heartbeat instead of starting at page 0.
    int startPage = ctx.getHeartbeatDetails(Integer.class).orElse(0);
    for (int page = startPage; page < pages; page++) {
      sleep(300);
      System.out.println("exported page " + (page + 1) + "/" + pages);
      ctx.heartbeat(page + 1);
    }
    return "exported " + pages + " pages";
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
