package training.temporal.tracing;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface OrderActivities {
  @ActivityMethod
  void validateOrder(String orderId);

  @ActivityMethod
  String chargePayment(String orderId);

  @ActivityMethod
  String shipOrder(String orderId);
}
