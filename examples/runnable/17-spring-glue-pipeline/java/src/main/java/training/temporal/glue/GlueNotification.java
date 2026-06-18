package training.temporal.glue;

/**
 * The message published to SNS to notify producer A. It is self-identifying
 * ({@code workflowId} + {@code runId}) so subscribers can dedup an at-least-once
 * SNS redelivery, and it carries references (the curated S3 URI), never the data.
 */
public record GlueNotification(
    String workflowId,
    String runId,
    String status,
    int fileCount,
    long totalBytes,
    String curatedS3Uri) {}
