package interactions

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// If the client times out or loses connectivity while starting a Workflow, the
// result is ambiguous: Temporal may have committed WorkflowExecutionStarted but
// the response never reached the caller. Retry with the same Workflow ID and
// treat "already started" as success.
func StartOrder(ctx context.Context, c client.Client, orderID string) error {
	workflowID := "order-" + orderID

	_, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:        workflowID,
			TaskQueue: "orders",
		},
		OrderWorkflow,
		orderID,
	)
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		fmt.Println("Already started; treating retry as success:", workflowID)
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Println("Started", workflowID)
	return nil
}

