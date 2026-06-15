package reliability

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// In Go, ExecuteActivity returns a Future immediately; .Get(ctx, &out) blocks
// for the result. Starting two extracts before getting either lets them run
// concurrently — the equivalent of Java's Async.function.
func AsyncWorkflow(ctx workflow.Context, batchDate string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	rawF := workflow.ExecuteActivity(ctx, "Extract", batchDate)
	auditF := workflow.ExecuteActivity(ctx, "Extract", batchDate+"-audit")

	var rawURI, auditURI string
	_ = rawF.Get(ctx, &rawURI)
	_ = auditF.Get(ctx, &auditURI)

	var cleanURI, cleanAuditURI string
	_ = workflow.ExecuteActivity(ctx, "Transform", rawURI).Get(ctx, &cleanURI)
	_ = workflow.ExecuteActivity(ctx, "Transform", auditURI).Get(ctx, &cleanAuditURI)

	_ = workflow.ExecuteActivity(ctx, "Load", cleanURI).Get(ctx, nil)
	return workflow.ExecuteActivity(ctx, "Load", cleanAuditURI).Get(ctx, nil)
}
