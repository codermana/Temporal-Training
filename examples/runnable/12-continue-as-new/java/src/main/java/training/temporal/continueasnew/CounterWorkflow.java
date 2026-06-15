package training.temporal.continueasnew;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface CounterWorkflow {
  /** @param processedSoFar carried over from the previous run via continue-as-new. */
  @WorkflowMethod
  String count(int processedSoFar);
}
