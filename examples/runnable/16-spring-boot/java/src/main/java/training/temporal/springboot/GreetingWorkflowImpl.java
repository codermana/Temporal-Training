package training.temporal.springboot;

import io.temporal.activity.ActivityOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import java.time.Duration;

/**
 * Discovered and registered automatically by the starter because of
 * {@link WorkflowImpl} — no {@code worker.registerWorkflowImplementationTypes(...)}
 * call anywhere. The {@code taskQueues} attribute is what tells the starter which
 * Worker (task queue) to host it on.
 */
@WorkflowImpl(taskQueues = SpringBootConstants.TASK_QUEUE)
public class GreetingWorkflowImpl implements GreetingWorkflow {

  private final GreetingActivities activities =
      Workflow.newActivityStub(
          GreetingActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(10))
              .build());

  private String status = "STARTED";

  @Override
  public String greet(String name) {
    status = "GREETING";
    String message = activities.composeGreeting(name);
    status = "DONE";
    return message;
  }

  @Override
  public String getStatus() {
    return status;
  }
}
