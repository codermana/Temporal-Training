package training.temporal.springboot;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * The web front door. Temporal isn't a framework you hand control to — it's a
 * client you call. The starter auto-configures the {@link WorkflowClient} bean;
 * we inject it and drive Workflows from plain HTTP handlers. The Worker polls in
 * the background, decoupled from these request threads.
 */
@RestController
@RequestMapping("/greetings")
public class GreetingController {

  private final WorkflowClient client;

  public GreetingController(WorkflowClient client) {
    this.client = client;
  }

  /** Start the Workflow and block for its result — synchronous request/response. */
  @PostMapping
  public GreetingResponse greet(@RequestBody GreetingRequest request) {
    GreetingWorkflow workflow =
        client.newWorkflowStub(
            GreetingWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(SpringBootConstants.TASK_QUEUE)
                .setWorkflowId("greeting-" + request.name())
                .build());

    // Blocks until the Workflow completes; for long Workflows you'd start async
    // and return an id instead (see the saga's Signal/Update patterns).
    String message = workflow.greet(request.name());
    return new GreetingResponse(message);
  }

  /** Query a Workflow's live status by its business id — no Activity, no replay. */
  @GetMapping("/{name}")
  public GreetingResponse status(@PathVariable String name) {
    WorkflowStub stub = client.newUntypedWorkflowStub("greeting-" + name);
    String status = stub.query("getStatus", String.class);
    return new GreetingResponse(status);
  }

  public record GreetingRequest(String name) {}

  public record GreetingResponse(String message) {}
}
