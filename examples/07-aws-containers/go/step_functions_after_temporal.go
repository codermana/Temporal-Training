// The Step Functions state machine (validate -> transform -> load -> notify),
// rewritten as one Temporal Workflow. The JSON state graph becomes straight-line
// Go; the Catch(NotifyFailure) becomes a deferred error check.
//
// The Go port of step_functions_after_temporal.java.
package aws

import (
	"context"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ImportActivities are the four states of the migrated pipeline. The bodies are
// illustrative; in the lab they wrap LocalStack S3/SNS calls.
type ImportActivities struct{}

func (ImportActivities) Validate(ctx context.Context, inputS3URI string) (string, error) {
	return inputS3URI, nil
}
func (ImportActivities) Transform(ctx context.Context, cleanS3URI string) (string, error) {
	return cleanS3URI, nil
}
func (ImportActivities) Load(ctx context.Context, transformedS3URI string) (string, error) {
	return transformedS3URI, nil
}
func (ImportActivities) Notify(ctx context.Context, message string) error { return nil }

// ImportWorkflow runs the four states in straight-line Go. The Step Functions
// Catch(NotifyFailure) branch becomes a deferred notify on error.
func ImportWorkflow(ctx workflow.Context, inputS3URI string) (err error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
	})

	var a *ImportActivities
	defer func() {
		if err != nil {
			// The Step Functions Catch(NotifyFailure) branch.
			_ = workflow.ExecuteActivity(ctx, a.Notify, "import failed").Get(ctx, nil)
		}
	}()

	var cleanURI string
	if err = workflow.ExecuteActivity(ctx, a.Validate, inputS3URI).Get(ctx, &cleanURI); err != nil {
		return err
	}
	var transformedURI string
	if err = workflow.ExecuteActivity(ctx, a.Transform, cleanURI).Get(ctx, &transformedURI); err != nil {
		return err
	}
	var loadResult string
	if err = workflow.ExecuteActivity(ctx, a.Load, transformedURI).Get(ctx, &loadResult); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, a.Notify, loadResult).Get(ctx, nil)
}
