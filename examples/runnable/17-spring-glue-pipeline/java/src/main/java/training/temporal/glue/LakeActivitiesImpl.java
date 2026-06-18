package training.temporal.glue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.activity.Activity;
import io.temporal.failure.ApplicationFailure;
import io.temporal.spring.boot.ActivityImpl;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * The Activity implementation — an ordinary Spring bean ({@code @Component}), so
 * the AWS clients, config, and JSON mapper all arrive by constructor injection.
 * {@link ActivityImpl} hands it to the starter, which registers it on the
 * {@code glue-stitch} task queue's Worker.
 *
 * <p>Every method here is non-deterministic I/O — listing S3, polling Glue,
 * publishing to SNS — which is exactly why it lives in an Activity and not in the
 * Workflow.
 */
@Component
@ActivityImpl(taskQueues = GlueStitchConstants.TASK_QUEUE)
public class LakeActivitiesImpl implements LakeActivities {

  private static final Logger log = LoggerFactory.getLogger(LakeActivitiesImpl.class);

  private final S3Client s3;
  private final SnsClient sns;
  private final GlueJobRunner glueJobRunner;
  private final ObjectMapper json;

  private final String glueJobName;
  private final String rawPrefix;
  private final String curatedPrefix;
  private final String notifyTopicArn;

  public LakeActivitiesImpl(
      S3Client s3,
      SnsClient sns,
      GlueJobRunner glueJobRunner,
      ObjectMapper json,
      @Value("${aws.glue-job-name:stitch-orders}") String glueJobName,
      @Value("${aws.raw-prefix:raw/orders/}") String rawPrefix,
      @Value("${aws.curated-prefix:curated/orders/}") String curatedPrefix,
      @Value("${aws.notify-topic-arn}") String notifyTopicArn) {
    this.s3 = s3;
    this.sns = sns;
    this.glueJobRunner = glueJobRunner;
    this.json = json;
    this.glueJobName = glueJobName;
    this.rawPrefix = rawPrefix;
    this.curatedPrefix = curatedPrefix;
    this.notifyTopicArn = notifyTopicArn;
  }

  @Override
  public PartitionManifest validatePartition(String bucket, String prefix) {
    ListObjectsV2Response resp =
        s3.listObjectsV2(ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());

    int fileCount = 0;
    long totalBytes = 0;
    String sampleKey = null;
    for (S3Object obj : resp.contents()) {
      if (!obj.key().endsWith(".parquet")) {
        continue; // ignore _SUCCESS markers and other non-data files
      }
      fileCount++;
      totalBytes += obj.size();
      if (sampleKey == null) {
        sampleKey = obj.key();
      }
    }

    if (fileCount == 0) {
      // A non-retryable failure: retrying an empty partition will never succeed.
      // The Workflow will surface this and stop, instead of looping forever.
      throw ApplicationFailure.newNonRetryableFailure(
          "no .parquet objects under s3://" + bucket + "/" + prefix, "EmptyPartition");
    }

    Activity.getExecutionContext().heartbeat("validated:" + fileCount);
    log.info("validated s3://{}/{} : {} parquet files, {} bytes", bucket, prefix, fileCount, totalBytes);
    return new PartitionManifest(fileCount, totalBytes, sampleKey);
  }

  @Override
  public String runGlueJob(StitchRequest request) {
    // Start + poll the (faked) Glue job, heartbeating with the run id so a
    // resumed Activity continues the poll rather than re-launching the job.
    String runId =
        glueJobRunner.runToCompletion(
            glueJobName,
            request.inputS3Uri(),
            rid -> Activity.getExecutionContext().heartbeat(rid));

    // The job's output is a curated partition mirroring the raw one. We write a
    // small marker object so the curated prefix is real and verifiable in S3.
    String curatedKey = request.prefix().replace(rawPrefix, curatedPrefix) + "part-0000.parquet";
    s3.putObject(
        PutObjectRequest.builder().bucket(request.bucket()).key(curatedKey).build(),
        RequestBody.fromString("STITCHED by Glue run " + runId, StandardCharsets.UTF_8));

    String curatedUri = "s3://" + request.bucket() + "/" + curatedKey;
    log.info("Glue run {} succeeded; curated output at {}", runId, curatedUri);
    return curatedUri;
  }

  @Override
  public String publishValidation(GlueNotification notification) {
    String body;
    try {
      body = json.writeValueAsString(notification);
    } catch (Exception e) {
      // Bad serialization is a bug, not a transient fault — don't retry forever.
      throw ApplicationFailure.newNonRetryableFailure(
          "could not serialize notification: " + e.getMessage(), "NotificationSerializationError");
    }

    // Standard SNS topic: at-least-once, so the workflowId in the body is what
    // subscribers dedup on (no FIFO dedup id here — that's FIFO-topic only).
    String messageId =
        sns.publish(
                PublishRequest.builder()
                    .topicArn(notifyTopicArn)
                    .subject("lake-validated")
                    .message(body)
                    .build())
            .messageId();

    log.info("published validation for {} -> SNS messageId={}", notification.workflowId(), messageId);
    return messageId;
  }
}
