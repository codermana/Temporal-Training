package saga

import (
	"context"

	"go.temporal.io/sdk/client"
)

// OrderRequest is the saga submission payload.
type OrderRequest struct {
	OrderID string
}

// SubmitOrder is a fire-and-forget submission. SignalWithStartWorkflow guarantees
// the workflow exists before the signal is delivered — without it the signal would
// race the start RPC and could land on a not-yet-created execution.
func SubmitOrder(ctx context.Context, c client.Client, request OrderRequest) error {
	_, err := c.SignalWithStartWorkflow(
		ctx,
		"order-"+request.OrderID,
		"submit", // signal name
		request,  // signal arg
		client.StartWorkflowOptions{ID: "order-" + request.OrderID, TaskQueue: "orders"},
		OrderSagaWorkflow,
		request.OrderID,
	)
	return err
}
