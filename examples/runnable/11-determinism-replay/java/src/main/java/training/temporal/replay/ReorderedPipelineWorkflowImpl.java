package training.temporal.replay;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * A "harmless looking" refactor that reorders the two Activities: load THEN extract. It implements
 * the SAME Workflow interface, so it has the same Workflow type name - which means a history
 * recorded from {@link DataPipelineWorkflowImpl} will be replayed against it, and the different
 * command order trips a non-determinism error. This is the regression replay testing catches.
 */
public class ReorderedPipelineWorkflowImpl implements DataPipelineWorkflow {

  private final PipelineActivities activities =
      Workflow.newActivityStub(
          PipelineActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(10)).build());

  @Override
  public String run() {
    String loaded = activities.load("rows:0");
    String extracted = activities.extract();
    return loaded + " / " + extracted;
  }
}
