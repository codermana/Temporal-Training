package training.temporal.replay;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface DataPipelineWorkflow {
  @WorkflowMethod
  String run();
}
