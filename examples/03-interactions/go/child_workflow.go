package interactions

import (
	"go.temporal.io/sdk/workflow"
)

// A parent fans out to child Workflows the same way it fans out to Activities:
// start every child before getting any, then collect. ExecuteChildWorkflow
// returns a ChildWorkflowFuture; pass a TaskQueue per child so each can run on
// its own Worker pool.
func ParentOrderWorkflow(ctx workflow.Context, orderID string) error {
	fraudCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{TaskQueue: "fraud"})
	shipCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{TaskQueue: "shipping"})

	fraud := workflow.ExecuteChildWorkflow(fraudCtx, FraudWorkflow, orderID)
	shipping := workflow.ExecuteChildWorkflow(shipCtx, ShippingWorkflow, orderID)

	var fraudDecision, shippingPlan string
	if err := fraud.Get(ctx, &fraudDecision); err != nil {
		return err
	}
	if err := shipping.Get(ctx, &shippingPlan); err != nil {
		return err
	}
	return nil
}
