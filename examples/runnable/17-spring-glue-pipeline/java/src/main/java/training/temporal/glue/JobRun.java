package training.temporal.glue;

/**
 * A snapshot of one stitch run — the shape the supervising Activity polls. It
 * mirrors AWS Glue's {@code JobRun}: a {@code JobRunState} plus, on success, the
 * location of the output (here the curated S3 URI) and, on failure, an error
 * message. {@link LocalStitchJobRunner} fills this in; a real
 * {@code GlueClient}-backed runner would map {@code GetJobRun} onto the same record.
 */
public record JobRun(State state, String curatedS3Uri, String errorMessage) {

  /** The subset of Glue's {@code JobRunState} the supervise loop reacts to. */
  public enum State {
    RUNNING,
    SUCCEEDED,
    FAILED
  }

  public static JobRun running() {
    return new JobRun(State.RUNNING, null, null);
  }

  public static JobRun succeeded(String curatedS3Uri) {
    return new JobRun(State.SUCCEEDED, curatedS3Uri, null);
  }

  public static JobRun failed(String errorMessage) {
    return new JobRun(State.FAILED, null, errorMessage);
  }
}
