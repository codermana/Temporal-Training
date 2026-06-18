package training.temporal.glue;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * The three I/O steps of the pipeline. Each is the boundary between deterministic
 * Workflow code and the non-deterministic outside world (S3, Glue, SNS).
 */
@ActivityInterface
public interface LakeActivities {

  /** List the Parquet objects under the raw prefix; fail (non-retryable) if empty. */
  @ActivityMethod
  PartitionManifest validatePartition(String bucket, String prefix);

  /** Start a Glue job over the raw partition, poll it to SUCCEEDED, return the curated S3 URI. */
  @ActivityMethod
  String runGlueJob(StitchRequest request);

  /** Publish the validation result to SNS so producer A is notified; return the SNS message id. */
  @ActivityMethod
  String publishValidation(GlueNotification notification);
}
