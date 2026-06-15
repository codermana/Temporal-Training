package training.temporal.replay;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.WorkflowExecutionHistory;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.testing.WorkflowReplayer;
import io.temporal.worker.Worker;
import org.junit.jupiter.api.Test;

class ReplayDeterminismTest {

  private static final String TASK_QUEUE = "replay-demo";
  private static final String WORKFLOW_ID = "pipeline-1";

  /** Run the shipped workflow once in-process and capture its event history. */
  private WorkflowExecutionHistory recordHistory() {
    try (TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance()) {
      Worker worker = env.newWorker(TASK_QUEUE);
      worker.registerWorkflowImplementationTypes(DataPipelineWorkflowImpl.class);
      worker.registerActivitiesImplementations(new PipelineActivitiesImpl());
      env.start();

      WorkflowClient client = env.getWorkflowClient();
      DataPipelineWorkflow workflow =
          client.newWorkflowStub(
              DataPipelineWorkflow.class,
              WorkflowOptions.newBuilder()
                  .setTaskQueue(TASK_QUEUE)
                  .setWorkflowId(WORKFLOW_ID)
                  .build());
      workflow.run();

      return client.fetchHistory(WORKFLOW_ID);
    }
  }

  @Test
  void shippedCodeReplaysClean() throws Exception {
    WorkflowExecutionHistory history = recordHistory();
    // No exception = the recorded history still matches the current code.
    WorkflowReplayer.replayWorkflowExecution(history, DataPipelineWorkflowImpl.class);
  }

  @Test
  void reorderedCodeBreaksReplay() throws Exception {
    WorkflowExecutionHistory history = recordHistory();
    // The reordered refactor produces different commands -> replay throws. CI would fail here.
    assertThrows(
        Exception.class,
        () -> WorkflowReplayer.replayWorkflowExecution(history, ReorderedPipelineWorkflowImpl.class));
  }
}
