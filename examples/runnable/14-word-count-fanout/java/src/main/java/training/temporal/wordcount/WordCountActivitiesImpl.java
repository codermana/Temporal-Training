package training.temporal.wordcount;

import io.temporal.activity.Activity;
import java.util.regex.Pattern;

public class WordCountActivitiesImpl implements WordCountActivities {
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  @Override
  public int countWords(String chunk) {
    Activity.getExecutionContext().heartbeat("counting chunk");
    String trimmed = chunk.trim();
    if (trimmed.isEmpty()) {
      return 0;
    }
    return WHITESPACE.split(trimmed).length;
  }
}
