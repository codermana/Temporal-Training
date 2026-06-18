package training.temporal.glue;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Validate a raw S3 partition, trigger + supervise a Glue job that stitches it,
 * then notify producer A. The three steps are Activities; this interface is the
 * deterministic contract.
 *
 * <p>{@link #triggerReceived} exists so the SQS bridge can use
 * {@code signalWithStart}: the first message <i>starts</i> the Workflow, and any
 * at-least-once redelivery of the same partition just <i>signals</i> the run that
 * is already in flight — no duplicate stitch. The Query exposes how many trigger
 * messages were absorbed, so you can see the dedup happening in the Web UI.
 */
@WorkflowInterface
public interface GlueStitchWorkflow {

  @WorkflowMethod
  StitchResult stitch(StitchRequest request);

  /** Idempotency hook for signalWithStart: a redelivered trigger lands here, not a new run. */
  @SignalMethod
  void triggerReceived(String messageId);

  /** Current step + how many trigger messages this run absorbed (readable live and after). */
  @QueryMethod
  String getStatus();

  @QueryMethod
  int getTriggerCount();
}
