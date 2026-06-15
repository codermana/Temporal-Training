package kafka

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Temporal owns the retry series. Once Validate exhausts its MaximumAttempts the
// Future's Get returns an error here — that's our signal to route the poison
// message to a Kafka DLQ instead of failing the Workflow.
func DlqRoutingWorkflow(ctx workflow.Context, orderID string) error {
	orderCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	if err := workflow.ExecuteActivity(orderCtx, "Validate", orderID).Get(orderCtx, nil); err != nil {
		dlqCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		})
		return workflow.ExecuteActivity(dlqCtx, "PublishToDLQ", orderID, err.Error()).Get(dlqCtx, nil)
	}
	return nil
}
