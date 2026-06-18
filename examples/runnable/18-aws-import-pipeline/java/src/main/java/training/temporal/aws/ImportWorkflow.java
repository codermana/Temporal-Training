package training.temporal.aws;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * The daily import pipeline: validate → transform → load → notify. This is the
 * Temporal replacement for a Step Functions state machine (Lab 6.3) — the JSON
 * state graph becomes straight-line code, {@code Retry} becomes {@code RetryOptions},
 * and {@code Catch(NotifyFailure)} becomes a try/catch that notifies failure.
 *
 * <p>{@link #fileArrived} is the idempotency hook for the SQS bridge (Lab 6.6):
 * {@code signalWithStart} <i>starts</i> the run on the first file-arrival message
 * and merely <i>signals</i> any at-least-once redelivery of the same file (keyed
 * on the Workflow ID) — no duplicate import. {@link #getTriggerCount} exposes how
 * many trigger messages were absorbed, so the dedup is visible in the Web UI.
 */
@WorkflowInterface
public interface ImportWorkflow {

  @WorkflowMethod
  String run(String inputS3Uri); // returns the output URI + row count, e.g. "s3://.../x.csv?rows=42"

  /** Idempotency hook for signalWithStart: a redelivered file-arrival lands here, not a new run. */
  @SignalMethod
  void fileArrived(String inputS3Uri);

  /** How many trigger messages this run absorbed (readable live and after completion). */
  @QueryMethod
  int getTriggerCount();
}
