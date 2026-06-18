package training.temporal.aws;

/**
 * Environment-driven configuration for the import pipeline. Every value has a
 * default that matches what {@code make aws-init} seeds into LocalStack, so the
 * module runs with zero flags once the stack is up; override any of them to point
 * at real AWS (clear {@link #AWS_ENDPOINT}) or at differently-named resources.
 *
 * <p>Note the split the labs care about: the <b>Temporal</b> connection settings
 * (address / namespace / task queue) are <i>not</i> here — they are loaded from
 * SSM Parameter Store at startup by {@link WorkerBootstrap} (Lab 6.8). This class
 * only holds the AWS-resource names the Activities act on.
 */
public final class Config {

  private Config() {}

  private static String env(String key, String fallback) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? fallback : v;
  }

  /** LocalStack endpoint; set to empty to use real AWS (default resolver + provider chain). */
  public static final String AWS_ENDPOINT = env("AWS_ENDPOINT", "http://127.0.0.1:4566");

  public static final String AWS_REGION = env("AWS_REGION", "us-east-1");

  // The three import buckets seeded by `make aws-init` (Labs 6.1–6.3). Each step
  // reads from one and writes to the next, passing S3 URIs forward, never bytes.
  public static final String BUCKET_INCOMING = env("BUCKET_INCOMING", "imports-incoming");
  public static final String BUCKET_VALIDATED = env("BUCKET_VALIDATED", "imports-validated");
  public static final String BUCKET_OUTPUT = env("BUCKET_OUTPUT", "imports-output");

  // SQS event-trigger queue (Lab 6.6). When set, WorkerMain also starts the bridge.
  public static final String IMPORTS_EVENTS_QUEUE_URL =
      env("IMPORTS_EVENTS_QUEUE_URL", AWS_ENDPOINT + "/000000000000/imports-events");

  // SNS completion topic (Lab 6.7).
  public static final String NOTIFY_TOPIC_ARN =
      env("NOTIFY_TOPIC_ARN", "arn:aws:sns:" + AWS_REGION + ":000000000000:imports-complete");

  // SSM Parameter Store tree the Worker config + secret live under (Lab 6.8).
  public static final String SSM_PATH = env("SSM_PATH", "/temporal-training/worker/");
  public static final String SSM_API_KEY =
      env("SSM_API_KEY", "/temporal-training/worker/api-key");
}
