package training.temporal.choreography;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderProcessWorkflow {
  @WorkflowMethod
  String run(String orderId);

  @SignalMethod
  void onEvent(DomainEvent event);

  @QueryMethod
  String status();
}

