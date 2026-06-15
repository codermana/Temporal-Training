package training.temporal.retries;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

public class ProcessingWorkflowImpl implements ProcessingWorkflow {

  // A deliberate retry policy: 5 attempts, 1s initial backoff doubling each time.
  private final FlakyActivities payment =
      Workflow.newActivityStub(
          FlakyActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(10))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setBackoffCoefficient(2.0)
                      .setMaximumAttempts(5)
                      .build())
              .build());

  // A long Activity: the heartbeat timeout detects a dead Worker between pages.
  private final FlakyActivities report =
      Workflow.newActivityStub(
          FlakyActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(5))
              .setHeartbeatTimeout(Duration.ofSeconds(5))
              .build());

  @Override
  public String process(String orderId) {
    String charge = payment.chargeCard(orderId);
    String exported = report.exportLargeReport(5);
    return charge + " | " + exported;
  }
}
