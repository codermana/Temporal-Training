package training.temporal.aws;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * The four steps of the import pipeline, each the boundary between deterministic
 * Workflow code and the non-deterministic outside world (S3, a long-running
 * transform job, SNS). Every step passes an S3 <b>URI</b> forward, never the file
 * bytes, so Workflow history stays small (Lab 6.2).
 */
@ActivityInterface
public interface ImportActivities {

  /** Read the incoming object, assert it is non-empty, copy it to the validated bucket. */
  @ActivityMethod
  String validate(String inputS3Uri);

  /** Supervise the long-running transform job (start → poll + heartbeat → settle); return output URI. */
  @ActivityMethod
  String transform(String validatedS3Uri);

  /** Read the transformed object, count its data rows, return the count (uses the SSM api-key at use-time). */
  @ActivityMethod
  long load(String transformedS3Uri);

  /** Publish the import result to SNS so subscribers fan out; return the SNS message id (Lab 6.7). */
  @ActivityMethod
  String publishNotification(String workflowId, String status, long rowCount, String outputS3Uri);
}
