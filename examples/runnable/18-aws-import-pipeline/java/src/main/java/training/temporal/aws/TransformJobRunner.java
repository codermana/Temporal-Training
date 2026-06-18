package training.temporal.aws;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * A self-hosted stand-in for an AWS Glue / long-running ETL job (Lab 6.1). Glue
 * is a paid-tier emulator on LocalStack, so instead of calling it this runner
 * does the same work for real against LocalStack S3: {@link #startJobRun} submits
 * the work to a background thread and returns a run id <b>immediately</b> (exactly
 * like Glue {@code StartJobRun}); the background task reads the validated object,
 * transforms it (here: upper-cases every data row), and writes the curated object
 * to the output bucket. {@link #getJobRun} reports the current {@link JobRun}.
 *
 * <p>The supervising Activity ({@link ImportActivitiesImpl#transform}) polls this
 * to a terminal state, heartbeating each poll — a loop that is byte-for-byte what
 * a real {@code GlueClient}-backed runner uses. Swapping in real Glue is a
 * one-class change: implement the same two methods over {@code GlueClient}.
 *
 * <p><b>Resume note:</b> run state lives in this process's memory, so a Worker
 * restart loses it and the Activity simply starts a fresh run — safe here because
 * the job rewrites the same deterministic output key (idempotent output).
 */
public final class TransformJobRunner {

  private static final Logger log = LoggerFactory.getLogger(TransformJobRunner.class);

  private final S3Client s3;
  private final long stepDelayMillis;

  // Daemon threads so an in-flight job never blocks JVM shutdown.
  private final ExecutorService jobs =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "transform-job");
            t.setDaemon(true);
            return t;
          });

  private final Map<String, JobRun> runs = new ConcurrentHashMap<>();

  public TransformJobRunner(S3Client s3, long stepDelayMillis) {
    this.s3 = s3;
    this.stepDelayMillis = stepDelayMillis;
  }

  /** Kick off the transform and return its run id immediately; poll {@link #getJobRun}. */
  public String startJobRun(String validatedS3Uri, String outputS3Uri) {
    String runId = "jr_" + Integer.toHexString(validatedS3Uri.hashCode());
    runs.put(runId, JobRun.running());
    log.info("StartJobRun input={} -> runId={}", validatedS3Uri, runId);
    jobs.submit(() -> transform(runId, validatedS3Uri, outputS3Uri));
    return runId;
  }

  /** The current state of a run — mirrors Glue {@code GetJobRun}. */
  public JobRun getJobRun(String runId) {
    return runs.getOrDefault(runId, JobRun.failed("unknown runId " + runId));
  }

  /** The job body: read validated CSV, upper-case data rows, write the curated object. Off-thread. */
  private void transform(String runId, String validatedS3Uri, String outputS3Uri) {
    try {
      S3Uri in = S3Uri.parse(validatedS3Uri);
      S3Uri out = S3Uri.parse(outputS3Uri);

      String content =
          s3.getObjectAsBytes(GetObjectRequest.builder().bucket(in.bucket()).key(in.key()).build())
              .asString(StandardCharsets.UTF_8);

      pause(); // pace the stage so the supervising Activity's heartbeats are visible

      String[] lines = content.split("\n", -1);
      StringBuilder transformed = new StringBuilder();
      for (int i = 0; i < lines.length; i++) {
        // Keep the header as-is; upper-case the data rows (a stand-in for real ETL).
        transformed.append(i == 0 ? lines[i] : lines[i].toUpperCase());
        if (i < lines.length - 1) {
          transformed.append('\n');
        }
      }

      s3.putObject(
          PutObjectRequest.builder().bucket(out.bucket()).key(out.key()).build(),
          RequestBody.fromString(transformed.toString(), StandardCharsets.UTF_8));

      runs.put(runId, JobRun.succeeded(outputS3Uri));
      log.info("transform runId={} SUCCEEDED -> {}", runId, outputS3Uri);
    } catch (Exception e) {
      // Any S3 error fails the run; the Activity maps this to a typed ApplicationFailure.
      runs.put(runId, JobRun.failed(e.getMessage()));
      log.warn("transform runId={} FAILED: {}", runId, e.toString());
    }
  }

  private void pause() {
    if (stepDelayMillis <= 0) {
      return;
    }
    try {
      Thread.sleep(stepDelayMillis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
