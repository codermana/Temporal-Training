package training.temporal.routing;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderWorkflow {
  /** Charge the order, then render its receipt, returning a one-line summary. */
  @WorkflowMethod
  String process(String orderId);
}
