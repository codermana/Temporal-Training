package training.temporal.replay;

public class PipelineActivitiesImpl implements PipelineActivities {
  @Override
  public String extract() {
    return "rows:100";
  }

  @Override
  public String load(String data) {
    return "loaded " + data;
  }
}
