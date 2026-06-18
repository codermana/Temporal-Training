package training.temporal.glue;

/**
 * What producer "A" tells us landed: a Parquet partition at {@code s3://bucket/prefix}.
 * Carried as the Workflow input and as the body of the SQS trigger message.
 */
public record StitchRequest(String bucket, String prefix) {
  /** The fully-qualified S3 URI of the raw partition. */
  public String inputS3Uri() {
    return "s3://" + bucket + "/" + prefix;
  }
}
