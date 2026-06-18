package aiml

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// An ML pipeline as a Workflow sequencing Activities: per-step retry policy, a
// quality gate written in code, heartbeat-based resume on the long training step,
// and idempotency via the deterministic WorkflowID passed into Activities. The
// Day-1 Airflow-vs-Temporal contrast, applied to model training.
func MLPipelineWorkflow(ctx workflow.Context, datasetURI string, accuracyThreshold float64) (string, error) {
	runID := workflow.GetInfo(ctx).WorkflowExecution.ID // deterministic → idempotent

	approved := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.GetSignalChannel(ctx, "approveDeploy").Receive(ctx, nil)
		approved = true
	})

	// Flaky source: retry generously.
	ingestCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 10},
	})
	var raw, features string
	if err := workflow.ExecuteActivity(ingestCtx, Ingest, datasetURI).Get(ctx, &raw); err != nil {
		return "", err
	}
	if err := workflow.ExecuteActivity(ingestCtx, Preprocess, raw).Get(ctx, &features); err != nil {
		return "", err
	}

	// Long GPU job: heartbeats let it resume from the last checkpoint on retry
	// instead of restarting from epoch 0. Cap attempts so a broken run doesn't
	// loop on GPUs.
	trainCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 12 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	var modelURI string
	if err := workflow.ExecuteActivity(trainCtx, Train, features, runID).Get(ctx, &modelURI); err != nil {
		return "", err
	}

	evalCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Minute,
	})
	var metrics Metrics
	if err := workflow.ExecuteActivity(evalCtx, Evaluate, modelURI).Get(ctx, &metrics); err != nil {
		return "", err
	}

	// Quality gate, in code: auto-deploy if clearly good, otherwise wait for a
	// human approval Signal before promoting a borderline model.
	if metrics.Accuracy < accuracyThreshold {
		_ = workflow.Await(ctx, func() bool { return approved })
	}

	var endpoint string
	if err := workflow.ExecuteActivity(evalCtx, Deploy, modelURI, runID).Get(ctx, &endpoint); err != nil {
		return "", err
	}
	return endpoint, nil
}
