package training.temporal.child;

import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.util.ArrayList;
import java.util.List;

public class BatchWorkflowImpl implements BatchWorkflow {

  @Override
  public String run(List<String> items) {
    List<Promise<String>> pending = new ArrayList<>();

    for (String item : items) {
      // Each child gets a stable, independent Workflow ID - you can Query/Signal/cancel it alone.
      ItemWorkflow child =
          Workflow.newChildWorkflowStub(
              ItemWorkflow.class,
              ChildWorkflowOptions.newBuilder().setWorkflowId("item-" + item).build());

      // Start all children in parallel; the parent suspends across all of them.
      pending.add(Async.function(child::processItem, item));
    }

    Promise.allOf(pending).get();

    StringBuilder out = new StringBuilder();
    for (Promise<String> p : pending) {
      out.append(p.get()).append('\n');
    }
    return out.toString().trim();
  }
}
