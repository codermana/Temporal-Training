package training.temporal.retries;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface ProcessingWorkflow {
  @WorkflowMethod
  String process(String orderId);
}
