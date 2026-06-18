package training.temporal.glue;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * Discovered and registered automatically by the starter because of
 * {@link WorkflowImpl} — there is no {@code registerWorkflowImplementationTypes}
 * call anywhere. {@code taskQueues} tells the starter which Worker hosts it.
 *
 * <p>The Workflow is the durable sequence: validate → stitch (Glue) → notify.
 * Each call is an Activity, so all the I/O (S3, Glue, SNS) lives outside Workflow
 * code and every step is retried independently with its own timeout. The Glue
 * step gets a long {@code startToCloseTimeout} and relies on heartbeats, because
 * a Spark job can run for many minutes.
 */
@WorkflowImpl(taskQueues = GlueStitchConstants.TASK_QUEUE)
public class GlueStitchWorkflowImpl implements GlueStitchWorkflow {

  // Short, cheap S3/SNS calls: quick timeout, a few retries.
  private final LakeActivities io =
      Workflow.newActivityStub(
          LakeActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setMaximumAttempts(5)
                      .build())
              .build());

  // The Glue job: a long-running supervised process. Generous start-to-close,
  // heartbeat timeout so a stuck poll is detected fast, capped retries.
  private final LakeActivities glue =
      Workflow.newActivityStub(
          LakeActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(30))
              .setHeartbeatTimeout(Duration.ofSeconds(30))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
              .build());

  private String status = "STARTED";
  private int triggerCount = 0;

  @Override
  public StitchResult stitch(StitchRequest request) {
    status = "VALIDATING";
    PartitionManifest manifest = io.validatePartition(request.bucket(), request.prefix());

    status = "STITCHING (Glue)";
    String curatedS3Uri = glue.runGlueJob(request);

    status = "NOTIFYING";
    String messageId =
        io.publishValidation(
            new GlueNotification(
                Workflow.getInfo().getWorkflowId(),
                Workflow.getInfo().getRunId(),
                "VALIDATED",
                manifest.fileCount(),
                manifest.totalBytes(),
                curatedS3Uri));

    status = "DONE";
    return new StitchResult(manifest.fileCount(), manifest.totalBytes(), curatedS3Uri, messageId);
  }

  @Override
  public void triggerReceived(String messageId) {
    // A redelivered SQS message for this same partition lands here instead of
    // starting a second run. We just count it; the work is already in flight.
    triggerCount++;
  }

  @Override
  public String getStatus() {
    return status;
  }

  @Override
  public int getTriggerCount() {
    return triggerCount;
  }
}
