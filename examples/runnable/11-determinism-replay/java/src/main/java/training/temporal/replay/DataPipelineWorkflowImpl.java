package training.temporal.replay;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/** The original, shipped version: extract THEN load. This is the order recorded in history. */
public class DataPipelineWorkflowImpl implements DataPipelineWorkflow {

  private final PipelineActivities activities =
      Workflow.newActivityStub(
          PipelineActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  @Override
  public String run() {
    String extracted = activities.extract();
    return activities.load(extracted);
  }
}
