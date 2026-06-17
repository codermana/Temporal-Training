package training.temporal.routing;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * One Workflow, two Activities, two different Worker pools. The Workflow itself
 * runs on the {@code orders} Task Queue; each Activity stub names the Task Queue
 * whose pool registers that Activity. No single Worker registers all of it.
 */
public class OrderWorkflowImpl implements OrderWorkflow {

  // Routed to the payments pool: cheap, high-replica; registers PaymentActivities.
  private final PaymentActivities pay =
      Workflow.newActivityStub(
          PaymentActivities.class,
          ActivityOptions.newBuilder()
              .setTaskQueue(TaskQueues.PAYMENTS)
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .build());

  // Routed to the media pool: scarce GPU boxes; registers MediaActivities.
  private final MediaActivities media =
      Workflow.newActivityStub(
          MediaActivities.class,
          ActivityOptions.newBuilder()
              .setTaskQueue(TaskQueues.MEDIA)
              .setStartToCloseTimeout(Duration.ofMinutes(5))
              .build());

  @Override
  public String process(String orderId) {
    String charged = pay.charge(orderId); // dispatched to the "payments" queue
    String receipt = media.render(orderId); // dispatched to the "media" queue
    return orderId + ": " + charged + " / " + receipt;
  }
}
