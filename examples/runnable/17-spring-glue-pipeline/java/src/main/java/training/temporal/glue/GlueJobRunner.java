package training.temporal.glue;

import java.util.function.Consumer;

/**
 * Supervises one external Glue job run: start it, then poll until it reaches a
 * terminal state, heartbeating the Temporal Activity on every poll so a stuck job
 * is detected. Returns the Glue job-run id; throws if the job ends in a failed
 * state.
 *
 * <p>This interface is the seam that keeps the lab runnable: LocalStack Community
 * has no Glue, so {@link FakeGlueJobRunner} simulates {@code StartJobRun} +
 * {@code GetJobRun} polling. On real AWS you swap in a {@code GlueClient}-backed
 * implementation (see README) — the Activity, Workflow, heartbeat, and retry
 * semantics are identical.
 */
public interface GlueJobRunner {

  /**
   * @param heartbeat called on every poll with the job-run id; wire it to
   *     {@code Activity.getExecutionContext().heartbeat(...)} so a resumed
   *     Activity can pick the poll back up instead of re-running the job.
   * @return the Glue job-run id once it reaches SUCCEEDED.
   */
  String runToCompletion(String jobName, String inputS3Uri, Consumer<String> heartbeat);
}
