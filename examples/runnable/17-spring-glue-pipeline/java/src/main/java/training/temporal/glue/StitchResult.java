package training.temporal.glue;

/** The Workflow's final result — what was stitched, where it landed, and the notify message id. */
public record StitchResult(
    int fileCount, long totalBytes, String curatedS3Uri, String notificationMessageId) {}
