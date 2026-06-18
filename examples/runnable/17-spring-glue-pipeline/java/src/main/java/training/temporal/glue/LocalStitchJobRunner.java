package training.temporal.glue;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A self-hosted stand-in for an AWS Glue job — it runs with zero paid services and
 * never touches real AWS. {@link #startJobRun} submits the work to a background
 * executor and returns a run id immediately; the background task does a
 * <b>genuine</b> S3 read → merge → write against LocalStack: it reads every raw
 * {@code .parquet} part under the input prefix, concatenates the bytes, and writes
 * one curated part. Then it marks the run {@code SUCCEEDED} (or {@code FAILED} on
 * any error). {@link #getJobRun} reports the current state.
 *
 * <p>This is real ETL, just self-hosted instead of Spark-on-Glue — the bytes that
 * land in the curated prefix are really merged from the raw objects. The Temporal
 * side (start, poll, heartbeat, fail loudly) is byte-for-byte what a
 * {@code GlueClient}-backed runner does, which is why swapping in real Glue is a
 * one-class change (see the README).
 *
 * <p><b>Resume note:</b> run state lives in this process's memory, so a Worker
 * restart loses it and the supervising Activity simply starts a fresh run — safe
 * here because the job rewrites the same deterministic curated key (idempotent
 * output). A {@code GlueClient}-backed runner gets cross-restart resume for free
 * because Glue holds the run state, not the Worker.
 */
@Component
public class LocalStitchJobRunner implements GlueJobRunner {

  private static final Logger log = LoggerFactory.getLogger(LocalStitchJobRunner.class);

  private final S3Client s3;

  // Pace each merged part so the demo job spans several poll intervals — that is
  // what makes the Activity's heartbeats visible. A real Spark stage is minutes,
  // not milliseconds; this is purely to keep the local demo honest about timing.
  private final long stepDelayMillis;

  // Daemon threads so an in-flight job never blocks JVM shutdown.
  private final ExecutorService jobs =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "stitch-job");
            t.setDaemon(true);
            return t;
          });

  private final Map<String, JobRun> runs = new ConcurrentHashMap<>();

  public LocalStitchJobRunner(
      S3Client s3, @Value("${aws.glue-step-delay-millis:400}") long stepDelayMillis) {
    this.s3 = s3;
    this.stepDelayMillis = stepDelayMillis;
  }

  @Override
  public String startJobRun(String jobName, String bucket, String inputPrefix, String outputPrefix) {
    String runId = "jr_" + Integer.toHexString((jobName + "|" + bucket + "/" + inputPrefix).hashCode());
    runs.put(runId, JobRun.running());
    log.info("StartJobRun job={} input=s3://{}/{} -> runId={}", jobName, bucket, inputPrefix, runId);
    jobs.submit(() -> stitch(runId, bucket, inputPrefix, outputPrefix));
    return runId;
  }

  @Override
  public JobRun getJobRun(String runId) {
    return runs.getOrDefault(runId, JobRun.failed("unknown runId " + runId));
  }

  /** The job body: merge every raw part into one curated object. Runs off-thread. */
  private void stitch(String runId, String bucket, String inputPrefix, String outputPrefix) {
    try {
      ListObjectsV2Response listing =
          s3.listObjectsV2(
              ListObjectsV2Request.builder().bucket(bucket).prefix(inputPrefix).build());

      ByteArrayOutputStream merged = new ByteArrayOutputStream();
      int parts = 0;
      for (S3Object obj : listing.contents()) {
        if (!obj.key().endsWith(".parquet")) {
          continue; // skip _SUCCESS markers and other non-data files
        }
        pause(); // pace the stage so the supervising Activity heartbeats across polls
        merged.writeBytes(
            s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(obj.key()).build())
                .asByteArray());
        parts++;
        log.info("stitch runId={} merged {} ({} parts so far)", runId, obj.key(), parts);
      }

      if (parts == 0) {
        runs.put(
            runId, JobRun.failed("no .parquet objects under s3://" + bucket + "/" + inputPrefix));
        log.warn("stitch runId={} FAILED: empty input partition", runId);
        return;
      }

      String curatedKey = outputPrefix + "part-0000.parquet";
      s3.putObject(
          PutObjectRequest.builder().bucket(bucket).key(curatedKey).build(),
          RequestBody.fromBytes(merged.toByteArray()));

      String curatedUri = "s3://" + bucket + "/" + curatedKey;
      runs.put(runId, JobRun.succeeded(curatedUri));
      log.info("stitch runId={} SUCCEEDED -> {} ({} parts merged)", runId, curatedUri, parts);
    } catch (Exception e) {
      // Any S3 error fails the run; the Activity maps this to a typed ApplicationFailure.
      runs.put(runId, JobRun.failed(e.getMessage()));
      log.warn("stitch runId={} FAILED: {}", runId, e.toString());
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
