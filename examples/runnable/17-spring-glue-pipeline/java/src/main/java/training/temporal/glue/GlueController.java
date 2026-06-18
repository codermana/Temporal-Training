package training.temporal.glue;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * REST front door — the synchronous twin of the SQS bridge. Where the bridge is
 * the event-driven path (A drops a message on the bus), this is the "kick it off
 * by hand and watch it" path for demos and ops. The starter auto-configures the
 * {@link WorkflowClient}; we inject it and drive Workflows from plain HTTP.
 */
@RestController
@RequestMapping("/stitch")
public class GlueController {

  private final WorkflowClient client;

  public GlueController(WorkflowClient client) {
    this.client = client;
  }

  /** Start the stitch Workflow and block for its result (Glue is faked, so it's quick). */
  @PostMapping
  public StitchResult stitch(@RequestBody StitchRequest request) {
    GlueStitchWorkflow workflow =
        client.newWorkflowStub(
            GlueStitchWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(GlueStitchConstants.TASK_QUEUE)
                .setWorkflowId("stitch-rest-" + slug(request.bucket() + "-" + request.prefix()))
                .build());
    try {
      // Blocks until the Workflow completes. For a real (minutes-long) Glue job
      // you'd start async and return the id, then poll the status endpoint below.
      return workflow.stitch(request);
    } catch (WorkflowExecutionAlreadyStarted e) {
      throw new ResponseStatusException(
          HttpStatus.CONFLICT, "a stitch for this partition is already running");
    }
  }

  /** Read a running (or finished) Workflow's status by its business id — a Query, no replay. */
  @GetMapping("/{workflowId}")
  public StatusResponse status(@PathVariable String workflowId) {
    WorkflowStub stub = client.newUntypedWorkflowStub(workflowId);
    return new StatusResponse(
        workflowId,
        stub.query("getStatus", String.class),
        stub.query("getTriggerCount", Integer.class));
  }

  public record StatusResponse(String workflowId, String status, int triggerCount) {}

  private static String slug(String s) {
    return s.replaceAll("[^A-Za-z0-9_.-]", "-").replaceAll("-+", "-").replaceAll("(^-|-$)", "");
  }
}
