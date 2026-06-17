package training.temporal.routing;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface MediaActivities {
  @ActivityMethod
  String render(String orderId);
}
