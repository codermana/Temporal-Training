package training.temporal.tracing;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderWorkflow {
  @WorkflowMethod
  String process(String orderId);
}
