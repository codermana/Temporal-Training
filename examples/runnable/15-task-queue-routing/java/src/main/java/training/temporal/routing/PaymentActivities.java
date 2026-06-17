package training.temporal.routing;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface PaymentActivities {
  @ActivityMethod
  String charge(String orderId);
}
