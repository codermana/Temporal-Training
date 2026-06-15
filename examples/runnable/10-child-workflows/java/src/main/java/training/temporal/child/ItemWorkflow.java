package training.temporal.child;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/** A child Workflow - its own Workflow ID, its own history, separately addressable. */
@WorkflowInterface
public interface ItemWorkflow {
  @WorkflowMethod
  String processItem(String item);
}
