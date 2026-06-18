package training.temporal.tracing;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * A three-step order pipeline. Nothing here is tracing-specific: the spans come
 * entirely from the interceptors wired in {@link Telemetry}. Each Activity call
 * becomes a child span under the Workflow span, which is itself a child of the
 * client's "start" span — so one Jaeger trace shows the whole run.
 */
public class OrderWorkflowImpl implements OrderWorkflow {
  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  @Override
  public String process(String orderId) {
    activities.validateOrder(orderId);
    String paymentId = activities.chargePayment(orderId);
    String tracking = activities.shipOrder(orderId);
    return "order " + orderId + " complete (payment=" + paymentId + ", tracking=" + tracking + ")";
  }
}
