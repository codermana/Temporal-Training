package reliability

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Activity timeouts and retry policy live together on ActivityOptions, the same
// knobs as the Java ActivityOptions builder.
func ActivityOptionsExample(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		// One attempt must finish inside this window.
		StartToCloseTimeout: 5 * time.Minute,
		// The whole retry series must finish inside this window.
		ScheduleToCloseTimeout: 30 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    6,
		},
	})
}
