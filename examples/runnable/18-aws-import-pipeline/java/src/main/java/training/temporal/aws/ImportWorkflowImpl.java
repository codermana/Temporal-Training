package training.temporal.aws;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * Translates the Step Functions state machine (Lab 6.3) into one Workflow:
 *
 * <ul>
 *   <li>each {@code Task} state → an Activity call,
 *   <li>{@code Retry} JSON → {@link RetryOptions},
 *   <li>{@code Catch(NotifyFailure)} → a try/catch that calls {@code notify} and rethrows.
 * </ul>
 *
 * Two activity stubs because the steps have different shapes: the quick S3/SNS
 * calls get a short timeout and several retries; the long-running transform job
 * (Lab 6.1) gets a generous start-to-close, a short heartbeat timeout (so a stuck
 * poll is caught fast), and capped retries.
 */
public class ImportWorkflowImpl implements ImportWorkflow {

  // Quick S3 / SNS calls: short timeout, a few retries.
  private final ImportActivities io =
      Workflow.newActivityStub(
          ImportActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(2)) // SFN IntervalSeconds
                      .setBackoffCoefficient(2.0) // SFN BackoffRate
                      .setMaximumAttempts(3) // SFN MaxAttempts
                      .build())
              .build());

  // The supervised transform job: a long-running process. Generous start-to-close,
  // a heartbeat timeout so a stuck poll is detected fast, capped retries.
  private final ImportActivities job =
      Workflow.newActivityStub(
          ImportActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(30))
              .setHeartbeatTimeout(Duration.ofSeconds(30))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
              .build());

  private int triggerCount = 0;

  @Override
  public String run(String inputS3Uri) {
    String workflowId = Workflow.getInfo().getWorkflowId();
    try {
      String validatedUri = io.validate(inputS3Uri);
      String transformedUri = job.transform(validatedUri);
      long rowCount = io.load(transformedUri);

      // NotifySuccess.
      io.publishNotification(workflowId, "VALIDATED", rowCount, transformedUri);
      return transformedUri + "?rows=" + rowCount;
    } catch (Exception e) {
      // Step Functions Catch(NotifyFailure): notify, then let the failure surface.
      io.publishNotification(workflowId, "FAILED", 0, inputS3Uri);
      throw e;
    }
  }

  @Override
  public void fileArrived(String inputS3Uri) {
    // A redelivered SQS message for this same file lands here instead of starting a
    // second run. We just count it; the import is already in flight (Lab 6.6).
    triggerCount++;
  }

  @Override
  public int getTriggerCount() {
    return triggerCount;
  }
}
