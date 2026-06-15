package interactions

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Update-with-Start atomically starts the Workflow (if not already running) and
// applies an Update in one round trip - the "add to cart, creating the cart if
// needed" pattern. The start half is a WithStartWorkflowOperation; the conflict
// policy USE_EXISTING reuses a running cart instead of erroring.
func AddItemOrCreateCart(ctx context.Context, c client.Client) (int, error) {
	startOp := c.NewWithStartWorkflowOperation(
		client.StartWorkflowOptions{
			ID:                       "cart-1001",
			TaskQueue:                "carts",
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		CartWorkflow,
		"cart-1001",
	)

	handle, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   "addItem",
			Args:         []any{"lamp", 1},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return 0, err
	}

	var itemCount int
	if err := handle.Get(ctx, &itemCount); err != nil {
		return 0, err
	}
	return itemCount, nil
}
