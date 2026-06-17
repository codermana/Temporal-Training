package training.temporal.springboot;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * A deliberately tiny Workflow: one Activity call, plus a Query so the REST API
 * can read live status. The point of this lab is the Spring Boot wiring, not the
 * Workflow logic.
 */
@WorkflowInterface
public interface GreetingWorkflow {
  @WorkflowMethod
  String greet(String name);

  /** Current step, readable while the Workflow runs and after it completes. */
  @QueryMethod
  String getStatus();
}
