package training.temporal.aws;

import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.ssm.SsmClient;

/**
 * The import Activities, doing <b>real</b> LocalStack S3 / SNS work. Each S3 step
 * reads from one bucket and writes the next, returning the new S3 URI rather than
 * the bytes (Lab 6.2). {@code transform} supervises a long-running job and
 * heartbeats (Lab 6.1). {@code publishNotification} publishes to SNS (Lab 6.7).
 * {@code load} reads the api-key secret <i>at use-time inside the Activity</i>
 * (Lab 6.8) — never in Workflow code, and never into Workflow history.
 */
public class ImportActivitiesImpl implements ImportActivities {

  private static final Logger log = LoggerFactory.getLogger(ImportActivitiesImpl.class);

  private final S3Client s3;
  private final SnsClient sns;
  private final SsmClient ssm;
  private final TransformJobRunner transformJob;
  private final long pollMillis;

  public ImportActivitiesImpl(
      S3Client s3, SnsClient sns, SsmClient ssm, TransformJobRunner transformJob, long pollMillis) {
    this.s3 = s3;
    this.sns = sns;
    this.ssm = ssm;
    this.transformJob = transformJob;
    this.pollMillis = pollMillis;
  }

  @Override
  public String validate(String inputS3Uri) {
    S3Uri in = S3Uri.parse(inputS3Uri);
    byte[] bytes =
        s3.getObjectAsBytes(GetObjectRequest.builder().bucket(in.bucket()).key(in.key()).build())
            .asByteArray();

    if (bytes.length == 0) {
      // Retrying an empty object will never succeed → non-retryable, surfaced and stopped.
      throw ApplicationFailure.newNonRetryableFailure(
          "empty object at " + inputS3Uri, "EmptyImport");
    }

    String validatedUri = S3Uri.of(Config.BUCKET_VALIDATED, in.key());
    S3Uri out = S3Uri.parse(validatedUri);
    s3.putObject(
        PutObjectRequest.builder().bucket(out.bucket()).key(out.key()).build(),
        RequestBody.fromBytes(bytes));

    Activity.getExecutionContext().heartbeat("validated");
    log.info("validated {} ({} bytes) -> {}", inputS3Uri, bytes.length, validatedUri);
    return validatedUri;
  }

  @Override
  public String transform(String validatedS3Uri) {
    S3Uri in = S3Uri.parse(validatedS3Uri);
    String outputUri = S3Uri.of(Config.BUCKET_OUTPUT, in.key());

    // Start the job (returns immediately, like Glue StartJobRun), then SUPERVISE it:
    // poll to a terminal state, heartbeating the run id on every poll so Temporal
    // can tell a stuck job from a slow one (and a resumed Activity continues the
    // poll rather than re-launching). This loop is identical to a real
    // GlueClient-backed runner — only the runner behind it differs (Lab 6.1).
    String runId = transformJob.startJobRun(validatedS3Uri, outputUri);

    while (true) {
      Activity.getExecutionContext().heartbeat(runId);
      JobRun run = transformJob.getJobRun(runId);
      switch (run.state()) {
        case SUCCEEDED -> {
          log.info("transform run {} succeeded -> {}", runId, run.outputS3Uri());
          return run.outputS3Uri();
        }
        case FAILED ->
            throw ApplicationFailure.newFailure(
                "transform job " + runId + " failed: " + run.errorMessage(), "TransformJobFailed");
        default -> backoff(); // RUNNING: wait, then poll again
      }
    }
  }

  @Override
  public long load(String transformedS3Uri) {
    // Read the downstream API key at USE-TIME, inside the Activity (Lab 6.8). It is
    // never read in Workflow code and never logged in full. In ECS/EKS the SSM
    // client authenticates from the task role / IRSA — no static keys in the image.
    String apiKey = Secrets.apiKey(ssm);
    log.info("load using api-key {} (read at use-time from SSM)", Secrets.mask(apiKey));

    S3Uri in = S3Uri.parse(transformedS3Uri);
    String content =
        s3.getObjectAsBytes(GetObjectRequest.builder().bucket(in.bucket()).key(in.key()).build())
            .asString(StandardCharsets.UTF_8);

    long rows = content.lines().filter(l -> !l.isBlank()).count();
    long dataRows = rows > 0 ? rows - 1 : 0; // drop the header row
    log.info("loaded {} ({} data rows)", transformedS3Uri, dataRows);
    return dataRows;
  }

  @Override
  public String publishNotification(
      String workflowId, String status, long rowCount, String outputS3Uri) {
    // Small JSON: workflowId (+ runId) is what at-least-once subscribers dedup on;
    // it carries a reference (the output URI), never the imported data (Lab 6.7).
    String runId = Activity.getExecutionContext().getInfo().getRunId();
    String body =
        String.format(
            "{\"workflowId\":\"%s\",\"runId\":\"%s\",\"status\":\"%s\",\"rowCount\":%d,\"outputS3Uri\":\"%s\"}",
            workflowId, runId, status, rowCount, outputS3Uri);

    String messageId =
        sns.publish(
                PublishRequest.builder()
                    .topicArn(Config.NOTIFY_TOPIC_ARN)
                    .subject("import-complete")
                    .message(body)
                    .build())
            .messageId();

    log.info("published {} for {} -> SNS messageId={}", status, workflowId, messageId);
    return messageId;
  }

  /** Back off between polls. {@code Thread.sleep} is fine here — Activity code, not Workflow code. */
  private void backoff() {
    try {
      Thread.sleep(pollMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw ApplicationFailure.newFailure(
          "interrupted while polling the transform job", "TransformPollingInterrupted");
    }
  }
}
