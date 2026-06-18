package training.temporal.aws;

import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/**
 * The event-driven trigger (Lab 6.6): a file lands in S3 and a message arrives on
 * the SQS {@code imports-events} queue; this bridge turns each message into a
 * {@code signalWithStart} on {@link ImportWorkflow}. In AWS this whole class is the
 * "EventBridge rule → Lambda → StartExecution" chain; here it is one long-poll
 * consumer.
 *
 * <p>It is plain <b>glue code</b>, not a Workflow or Activity: it runs on its own
 * daemon thread, outside Temporal's determinism sandbox, so blocking SDK calls and
 * loops are fine. {@code signalWithStart} is idempotent on the Workflow ID, so
 * SQS's at-least-once redelivery of the same file re-signals the one run (bumping
 * {@code fileArrived}) instead of starting a duplicate. The message is deleted only
 * <i>after</i> the signal is durable in Temporal — at-least-once, never at-most-once.
 */
public final class SqsSignalBridge implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(SqsSignalBridge.class);

  private final WorkflowClient client;
  private final SqsClient sqs;
  private final String queueUrl;
  private final String taskQueue;
  private volatile boolean running = true;

  public SqsSignalBridge(WorkflowClient client, SqsClient sqs, String queueUrl, String taskQueue) {
    this.client = client;
    this.sqs = sqs;
    this.queueUrl = queueUrl;
    this.taskQueue = taskQueue;
  }

  public void stop() {
    running = false;
  }

  @Override
  public void run() {
    log.info("SQS trigger bridge polling {}", queueUrl);
    while (running) {
      try {
        ReceiveMessageResponse resp =
            sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20) // long poll, not a hot spin
                    .build());
        for (Message m : resp.messages()) {
          handle(m);
        }
      } catch (Exception e) {
        if (!running) {
          return; // interrupted during shutdown — expected
        }
        log.warn("SQS poll failed, backing off 2s: {}", e.toString());
        sleep(2000);
      }
    }
  }

  private void handle(Message m) {
    String s3Uri;
    try {
      s3Uri = parseS3Uri(m.body());
    } catch (Exception e) {
      // Unparseable message: log and delete so it doesn't poison the queue.
      log.warn("dropping unparseable trigger message {}: {}", m.messageId(), e.toString());
      delete(m);
      return;
    }

    // Workflow ID is a pure function of the file (its S3 key), so two messages for
    // the same file resolve to the same run: the first starts it, the rest signal it.
    String workflowId = "import-" + slug(S3Uri.parse(s3Uri).key());
    ImportWorkflow stub =
        client.newWorkflowStub(
            ImportWorkflow.class,
            WorkflowOptions.newBuilder().setWorkflowId(workflowId).setTaskQueue(taskQueue).build());

    BatchRequest batch = client.newSignalWithStartRequest();
    batch.add(stub::run, s3Uri);
    batch.add(stub::fileArrived, s3Uri);
    client.signalWithStart(batch);
    log.info("signalWithStart {} from SQS message {}", workflowId, m.messageId());

    // Delete only after the signal is durable in Temporal: at-least-once.
    delete(m);
  }

  /** Accept either a raw {@code s3://...} body or a JSON object with an {@code s3Uri} field. */
  private static String parseS3Uri(String body) {
    String trimmed = body.trim();
    if (trimmed.startsWith("s3://")) {
      return trimmed;
    }
    int i = trimmed.indexOf("\"s3Uri\"");
    if (i < 0) {
      throw new IllegalArgumentException("no s3Uri in message body");
    }
    int colon = trimmed.indexOf(':', i);
    int firstQuote = trimmed.indexOf('"', colon + 1);
    int secondQuote = trimmed.indexOf('"', firstQuote + 1);
    return trimmed.substring(firstQuote + 1, secondQuote);
  }

  private void delete(Message m) {
    sqs.deleteMessage(
        DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
  }

  /** Workflow IDs can't carry "/" cleanly; fold an S3 key to a safe slug. */
  private static String slug(String s) {
    return s.replaceAll("[^A-Za-z0-9_.-]", "-").replaceAll("-+", "-").replaceAll("(^-|-$)", "");
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
