package saga

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// SubmitOrderSync is sync request/response over a saga. UpdateWithStartWorkflow
// creates the workflow if it does not exist and applies the update in one round
// trip — the Go equivalent of Java's startUpdateWithStart — so the caller can wait
// on a result without a separate start RPC.
func SubmitOrderSync(ctx context.Context, c client.Client, request OrderRequest) (string, error) {
	startOp := c.NewWithStartWorkflowOperation(
		client.StartWorkflowOptions{
			ID:                       "order-" + request.OrderID,
			TaskQueue:                "orders",
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		OrderSagaWorkflow,
		request.OrderID,
	)

	handle, err := c.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName:   "submit",
			Args:         []interface{}{request},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return "", err
	}

	var result string
	if err := handle.Get(ctx, &result); err != nil {
		return "", err
	}
	return result, nil
}
