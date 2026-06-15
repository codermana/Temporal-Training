package training.temporal.replay;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface PipelineActivities {
  @ActivityMethod
  String extract();

  @ActivityMethod
  String load(String data);
}
