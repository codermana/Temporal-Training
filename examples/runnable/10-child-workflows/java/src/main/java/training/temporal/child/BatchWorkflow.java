package training.temporal.child;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.List;

@WorkflowInterface
public interface BatchWorkflow {
  @WorkflowMethod
  String run(List<String> items);
}
