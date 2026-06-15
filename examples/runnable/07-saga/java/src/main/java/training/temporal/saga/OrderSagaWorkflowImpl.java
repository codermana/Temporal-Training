package training.temporal.saga;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;

public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {
  // Bound the retries so a permanent failure exhausts quickly and the catch block
  // can run the compensations. Without a maximumAttempts the default policy retries
  // forever and the saga would never reach compensation.
  private final OrderActivities activities =
      Workflow.newActivityStub(
          OrderActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofMillis(500))
                      .setMaximumAttempts(3)
                      .build())
              .build());

  @Override
  public String process(String orderId) {
    Deque<Runnable> compensations = new ArrayDeque<>();

    try {
      String paymentId = activities.authorizePayment(orderId);
      compensations.push(() -> activities.cancelPayment(paymentId));

      String reservationId = activities.reserveInventory(orderId);
      compensations.push(() -> activities.restoreInventory(reservationId));

      activities.ship(orderId);
      return "COMPLETED";
    } catch (RuntimeException failure) {
      while (!compensations.isEmpty()) {
        compensations.pop().run();
      }
      activities.sendFailureNotification(orderId, failure.getMessage());
      return "COMPENSATED";
    }
  }
}

