package training.temporal.glue;

/**
 * The seam for the external compute that stitches a raw partition into a curated
 * one. We never call AWS Glue: it is a <b>paid-tier emulator</b> on LocalStack
 * (and a real, billed Spark service on AWS). Instead {@link LocalStitchJobRunner}
 * is a self-hosted stand-in that does the same work — a real S3 read → merge →
 * write — against LocalStack, with zero paid services and no real-AWS calls.
 *
 * <p>The two methods map one-to-one onto Glue's {@code StartJobRun} /
 * {@code GetJobRun}: {@link #startJobRun} kicks off the work and returns
 * immediately; {@link #getJobRun} reports the current {@link JobRun.State}. Because
 * the seam matches Glue's, the Activity's supervise loop (start → poll + heartbeat
 * → map terminal state) is identical whether the runner is local or a
 * {@code GlueClient}-backed implementation. Swapping in real Glue is a one-class
 * change (see the README).
 */
public interface GlueJobRunner {

  /**
   * Kick off a stitch run and return its id <b>immediately</b> — this does NOT
   * block until the job finishes, exactly like Glue {@code StartJobRun}. The work
   * runs on a background thread; poll {@link #getJobRun(String)} for its state.
   *
   * @param outputPrefix the curated prefix the run writes under (the runner
   *     appends the object name)
   * @return the run id to poll and to heartbeat from the Activity
   */
  String startJobRun(String jobName, String bucket, String inputPrefix, String outputPrefix);

  /** The current state of a run — mirrors Glue {@code GetJobRun}. */
  JobRun getJobRun(String runId);
}
