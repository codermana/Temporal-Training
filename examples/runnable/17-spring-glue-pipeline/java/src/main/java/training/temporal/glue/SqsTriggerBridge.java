package training.temporal.glue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/**
 * The event-driven trigger: producer A drops a message on an SQS "bus" when it
 * has landed a Parquet partition, and this bridge turns each message into a
 * {@code signalWithStart} on {@link GlueStitchWorkflow}.
 *
 * <p>This is plain glue code, <b>not</b> a Workflow or Activity — it runs on its
 * own daemon thread, outside Temporal's determinism sandbox, so blocking SDK
 * calls and loops are fine. In AWS this whole class is the
 * "EventBridge rule → Lambda → StartExecution" chain; here it's one long-poll
 * consumer.
 *
 * <p>{@code signalWithStart} is idempotent on the Workflow ID, so SQS's
 * at-least-once redelivery of the same partition re-signals the one run (bumping
 * {@code triggerReceived}) instead of starting a duplicate stitch. The message is
 * deleted only <i>after</i> the signal is durable in Temporal — at-least-once,
 * never at-most-once.
 */
@Component
public class SqsTriggerBridge {

  private static final Logger log = LoggerFactory.getLogger(SqsTriggerBridge.class);

  private final WorkflowClient client;
  private final SqsClient sqs;
  private final ObjectMapper json;
  private final String queueUrl;
  private final boolean enabled;

  private volatile boolean running = false;
  private Thread pumpThread;

  public SqsTriggerBridge(
      WorkflowClient client,
      SqsClient sqs,
      ObjectMapper json,
      @Value("${aws.trigger-queue-url:}") String queueUrl,
      @Value("${aws.bridge.enabled:true}") boolean enabled) {
    this.client = client;
    this.sqs = sqs;
    this.json = json;
    this.queueUrl = queueUrl;
    this.enabled = enabled;
  }

  @PostConstruct
  void start() {
    if (!enabled || queueUrl.isBlank()) {
      log.info("SQS trigger bridge disabled (aws.bridge.enabled={}, queueUrl set={}).", enabled, !queueUrl.isBlank());
      return;
    }
    running = true;
    pumpThread = new Thread(this::pump, "sqs-trigger-bridge");
    pumpThread.setDaemon(true);
    pumpThread.start();
    log.info("SQS trigger bridge polling {}", queueUrl);
  }

  @PreDestroy
  void stop() {
    running = false;
    if (pumpThread != null) {
      pumpThread.interrupt();
    }
  }

  private void pump() {
    while (running) {
      try {
        var resp =
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
    StitchRequest request;
    try {
      request = json.readValue(m.body(), StitchRequest.class);
    } catch (Exception e) {
      // Unparseable message: log and delete so it doesn't poison the queue.
      log.warn("dropping unparseable trigger message {}: {}", m.messageId(), e.toString());
      delete(m);
      return;
    }

    String workflowId = "stitch-" + slug(request.bucket() + "-" + request.prefix());
    GlueStitchWorkflow stub =
        client.newWorkflowStub(
            GlueStitchWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)
                .setTaskQueue(GlueStitchConstants.TASK_QUEUE)
                .build());

    // signalWithStart: start the Workflow if absent (stitch), and signal it
    // (triggerReceived). A redelivered message re-signals the same run.
    BatchRequest batch = client.newSignalWithStartRequest();
    batch.add(stub::stitch, request);
    batch.add(stub::triggerReceived, m.messageId());
    client.signalWithStart(batch);
    log.info("signalWithStart {} from SQS message {}", workflowId, m.messageId());

    // Delete only after the signal is durable in Temporal.
    delete(m);
  }

  private void delete(Message m) {
    sqs.deleteMessage(
        DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build());
  }

  /** Workflow IDs can't contain "/" cleanly in URLs/UI; fold partition prefixes to a safe slug. */
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
