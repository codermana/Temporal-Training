package training.temporal.wordcount;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

@ActivityInterface
public interface WordCountActivities {
  @ActivityMethod
  int countWords(String chunk);
}
