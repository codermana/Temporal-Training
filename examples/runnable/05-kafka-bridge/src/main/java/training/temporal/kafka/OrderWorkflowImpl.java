package training.temporal.kafka;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

public class OrderWorkflowImpl implements OrderWorkflow {
  private final OutcomeActivities activities =
      Workflow.newActivityStub(
          OutcomeActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  private final Deque<String> events = new ArrayDeque<>();

  /**
   * One long-lived Workflow per order key. The first Kafka event starts it; every
   * later event for the same key is delivered as a Signal to this same execution
   * (via {@code signalWithStart} in {@link KafkaSignalBridge}) - it must not start a
   * new Workflow. The Workflow stays open, publishing an outcome per event, and
   * continue-as-news to keep its history bounded.
   */
  @Override
  public String run(String orderId) {
    int processed = 0;
    while (true) {
      Workflow.await(() -> !events.isEmpty());
      while (!events.isEmpty()) {
        String event = events.poll();
        activities.publishOutcome(orderId, "accepted:" + orderId + ":" + event);
        processed++;
      }
      if (processed >= 1000) {
        Workflow.continueAsNew(orderId);
      }
    }
  }

  @Override
  public void orderEvent(String payload) {
    events.add(payload);
  }
}
