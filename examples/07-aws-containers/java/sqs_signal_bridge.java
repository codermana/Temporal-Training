// An "EventBridge rule -> Lambda -> StartExecution" trigger becomes a long-poll
// SQS consumer that starts or signals Workflows. The bridge is plain glue code
// (it runs outside any Workflow): receive a message, translate it to a
// signalWithStart, delete it. signalWithStart is idempotent on the Workflow ID,
// so an at-least-once SQS redelivery just re-signals the same Workflow.
class SqsSignalBridge {
  private final WorkflowClient client;
  private final SqsClient sqs;
  private final String queueUrl;

  void pump() {
    while (true) {
      ReceiveMessageResponse resp =
          sqs.receiveMessage(
              ReceiveMessageRequest.builder()
                  .queueUrl(queueUrl)
                  .maxNumberOfMessages(10)
                  .waitTimeSeconds(20) // long poll, not a hot spin
                  .build());

      for (Message m : resp.messages()) {
        FileEvent event = parse(m.body());

        // signalWithStart: starts the Workflow if absent, signals it if running.
        ImportWorkflow stub =
            client.newWorkflowStub(
                ImportWorkflow.class,
                WorkflowOptions.newBuilder()
                    .setWorkflowId("import-" + event.bucket() + "-" + event.key())
                    .setTaskQueue("transform")
                    .build());
        BatchRequest batch = client.newSignalWithStartRequest();
        batch.add(stub::run, event.s3Uri());
        client.signalWithStart(batch);

        // Delete only after the signal is durable in Temporal — at-least-once.
        sqs.deleteMessage(
            DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(m.receiptHandle())
                .build());
      }
    }
  }
}
