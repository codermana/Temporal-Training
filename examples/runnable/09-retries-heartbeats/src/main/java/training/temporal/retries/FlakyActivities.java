package training.temporal.retries;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface FlakyActivities {

  /** Fails the first two attempts, then succeeds - so the retry policy is visible in history. */
  @ActivityMethod
  String chargeCard(String orderId);

  /** Long-running work that heartbeats once per page, so a Worker restart resumes mid-flight. */
  @ActivityMethod
  String exportLargeReport(int pages);
}
