// The final "notify" step of an import — in AWS an SNS publish wrapped in a
// Lambda (or an EventBridge fan-out) — becomes an Activity that publishes to an
// SNS topic. Publishing is I/O, so it lives in an Activity, never in Workflow
// code. Subscribers (email, SQS, Lambda) stay decoupled: the publisher only
// knows the topic ARN, not who's listening.
//
// SNS delivery is at-least-once, so a retried Activity may publish the same
// notification twice. Make the message self-identifying — include the
// workflowId + runId — so consumers can dedup, and on a FIFO topic pass a
// message-deduplication id (the workflowId) so SNS itself collapses duplicates.
class SnsPublishActivitiesImpl implements SnsPublishActivities {
  private final SnsClient sns;
  private final String topicArn;

  SnsPublishActivitiesImpl(SnsClient sns, String topicArn) {
    this.sns = sns;
    this.topicArn = topicArn;
  }

  @Override
  public String publishNotification(String workflowId, long rowCount, String outputS3Uri) {
    // The workflowId + runId travel in the body so subscribers can dedup an
    // at-least-once redelivery; the message is a small notification, not a
    // data bus — pass the output URI, never the rows themselves.
    String runId = Activity.getExecutionContext().getInfo().getRunId();
    String message =
        String.format(
            "{\"workflowId\":\"%s\",\"runId\":\"%s\",\"rowCount\":%d,\"outputS3Uri\":\"%s\"}",
            workflowId, runId, rowCount, outputS3Uri);

    PublishResponse resp =
        sns.publish(
            PublishRequest.builder()
                .topicArn(topicArn)
                .subject("import-complete")
                .message(message)
                // FIFO topic: the workflowId is the dedup id, so SNS collapses a
                // retried publish into one delivery. Harmless on a standard topic.
                .messageDeduplicationId(workflowId)
                .messageGroupId("imports")
                .build());

    return resp.messageId(); // surfaces in the UI as the Activity result
  }
}
