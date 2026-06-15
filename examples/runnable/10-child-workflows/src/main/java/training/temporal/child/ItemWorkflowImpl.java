package training.temporal.child;

import io.temporal.workflow.Workflow;
import java.time.Duration;

public class ItemWorkflowImpl implements ItemWorkflow {
  @Override
  public String processItem(String item) {
    // A durable sleep so each child is visibly "running" in the Web UI for a moment.
    Workflow.sleep(Duration.ofMillis(500));
    return "processed[" + item + "] in child " + Workflow.getInfo().getWorkflowId();
  }
}
