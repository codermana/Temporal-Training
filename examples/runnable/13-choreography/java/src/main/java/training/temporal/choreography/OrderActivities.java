package training.temporal.choreography;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface OrderActivities {
  @ActivityMethod
  void reserveLocalInventory(String orderId);

  @ActivityMethod
  void requestShipment(String orderId);

  @ActivityMethod
  void releaseInventory(String orderId, String reason);
}

