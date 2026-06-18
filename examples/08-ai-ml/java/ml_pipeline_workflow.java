// An ML pipeline as a Workflow sequencing Activities: per-step retry policy, a
// quality gate written in code, heartbeat-based resume on the long training step,
// and idempotency via the deterministic workflowId passed into Activities. The
// Day-1 Airflow-vs-Temporal contrast, applied to model training.
@WorkflowInterface
interface MLPipelineWorkflow {
  @WorkflowMethod
  String run(String datasetUri, double accuracyThreshold);

  @SignalMethod
  void approveDeploy();
}

class MLPipelineWorkflowImpl implements MLPipelineWorkflow {
  // Flaky source: retry generously.
  private final MLActivities ingestStub =
      Workflow.newActivityStub(
          MLActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(30))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(10).build())
              .build());

  // Long GPU job: heartbeats let it resume from the last checkpoint on retry
  // instead of restarting from epoch 0. Cap attempts so a broken run doesn't
  // loop on GPUs.
  private final MLActivities trainStub =
      Workflow.newActivityStub(
          MLActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofHours(12))
              .setHeartbeatTimeout(Duration.ofMinutes(5))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
              .build());

  private boolean approved = false;

  @Override
  public String run(String datasetUri, double accuracyThreshold) {
    String runId = Workflow.getInfo().getWorkflowId(); // deterministic → idempotent

    String raw = ingestStub.ingest(datasetUri);
    String features = ingestStub.preprocess(raw);
    String modelUri = trainStub.train(features, runId);
    Metrics metrics = trainStub.evaluate(modelUri);

    // Quality gate, in code: auto-deploy if clearly good, otherwise wait for a
    // human approval Signal before promoting a borderline model.
    if (metrics.accuracy() < accuracyThreshold) {
      Workflow.await(() -> approved);
    }
    return trainStub.deploy(modelUri, runId);
  }

  @Override
  public void approveDeploy() {
    approved = true;
  }
}
