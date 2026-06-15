package training.temporal.continueasnew;

import io.temporal.workflow.Workflow;
import java.time.Duration;

public class CounterWorkflowImpl implements CounterWorkflow {

  // Cap the work done in a single run so history stays small...
  private static final int BATCH_PER_RUN = 3;
  // ...and stop continuing once the whole job is done.
  private static final int TOTAL = 9;

  @Override
  public String count(int processedSoFar) {
    int processed = processedSoFar;
    int thisRun = 0;

    while (thisRun < BATCH_PER_RUN && processed < TOTAL) {
      Workflow.sleep(Duration.ofMillis(500));
      processed++;
      thisRun++;
      Workflow.getLogger(CounterWorkflowImpl.class)
          .info("processed {} of {} (this run: {})", processed, TOTAL, thisRun);
    }

    if (processed >= TOTAL) {
      return "completed after " + processed + " iterations";
    }

    // Start a fresh run with the same Workflow ID; carry forward only what the next run needs.
    Workflow.continueAsNew(processed);
    throw new IllegalStateException("unreachable: continueAsNew does not return");
  }
}
