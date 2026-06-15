package interactions

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
)

// Two distinct lifetimes on the start options:
//
//	WorkflowExecutionTimeout - the whole Workflow, summed across continue-as-new.
//	WorkflowRunTimeout       - one individual run before it must finish or CAN.
func Start(ctx context.Context, c client.Client) error {
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "orders-2026-05-27",
		TaskQueue: "orders",
		// Maximum lifetime across continue-as-new runs.
		WorkflowExecutionTimeout: 7 * 24 * time.Hour,
		// Maximum lifetime for this individual run.
		WorkflowRunTimeout: 12 * time.Hour,
	}, OrdersWorkflow, "2026-05-27")
	return err
}
