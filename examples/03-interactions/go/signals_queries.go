package interactions

import (
	"go.temporal.io/sdk/workflow"
)

// Signals arrive on a channel you read with workflow.GetSignalChannel; queries
// are pull handlers you register with workflow.SetQueryHandler. The Workflow
// blocks in workflow.Await (the Go equivalent of Workflow.await) until a Signal
// flips the state.
func HumanApprovalWorkflow(ctx workflow.Context, requestID string) (string, error) {
	state := "WAITING"

	if err := workflow.SetQueryHandler(ctx, "currentState", func() (string, error) {
		return state, nil
	}); err != nil {
		return "", err
	}

	approvals := workflow.GetSignalChannel(ctx, "approve")
	workflow.Go(ctx, func(ctx workflow.Context) {
		var approver string
		approvals.Receive(ctx, &approver)
		state = "APPROVED by " + approver
	})

	_ = workflow.Await(ctx, func() bool {
		return len(state) >= 8 && state[:8] == "APPROVED"
	})
	return state, nil
}
