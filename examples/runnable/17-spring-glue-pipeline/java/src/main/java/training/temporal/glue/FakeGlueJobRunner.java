package training.temporal.glue;

import io.temporal.failure.ApplicationFailure;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * A stand-in for AWS Glue that runs anywhere — no Glue service required (it is
 * Pro-only on LocalStack Community). It mimics the real shape: a
 * {@code StartJobRun}, then a poll loop that transitions RUNNING → SUCCEEDED,
 * heartbeating each time. The supervised-compute pattern (start, poll, heartbeat,
 * fail loudly) is exactly what a {@code GlueClient}-backed runner does on real AWS.
 *
 * <p>The real version (drop-in replacement) is roughly:
 *
 * <pre>{@code
 * String runId = glue.startJobRun(StartJobRunRequest.builder()
 *     .jobName(jobName).arguments(Map.of("--input", inputS3Uri)).build()).jobRunId();
 * while (true) {
 *   heartbeat.accept(runId);
 *   JobRun run = glue.getJobRun(GetJobRunRequest.builder()
 *       .jobName(jobName).runId(runId).build()).jobRun();
 *   switch (run.jobRunState()) {
 *     case SUCCEEDED -> { return runId; }
 *     case FAILED, TIMEOUT, STOPPED -> throw ApplicationFailure.newFailure(...);
 *     default -> Thread.sleep(15_000);
 *   }
 * }
 * }</pre>
 */
@Component
public class FakeGlueJobRunner implements GlueJobRunner {

  private static final Logger log = LoggerFactory.getLogger(FakeGlueJobRunner.class);

  // How many "RUNNING" polls before SUCCEEDED, and how long each poll waits.
  // Short so the demo is snappy; a real Spark job is minutes, hence heartbeats.
  private static final int POLLS = 5;
  private static final long POLL_MILLIS = 400;

  @Override
  public String runToCompletion(String jobName, String inputS3Uri, Consumer<String> heartbeat) {
    String runId = "jr_" + Integer.toHexString((jobName + "|" + inputS3Uri).hashCode());
    log.info("Glue StartJobRun job={} input={} -> runId={}", jobName, inputS3Uri, runId);

    for (int poll = 1; poll <= POLLS; poll++) {
      heartbeat.accept(runId); // resume picks up here instead of re-running the job
      log.info("Glue GetJobRun runId={} state=RUNNING ({}/{})", runId, poll, POLLS);
      try {
        Thread.sleep(POLL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw ApplicationFailure.newFailure(
            "interrupted while polling Glue job " + runId, "GluePollingInterrupted");
      }
    }

    log.info("Glue GetJobRun runId={} state=SUCCEEDED", runId);
    return runId;
  }
}
