// Package approval holds the approval lab: signal, query, and update on one
// running execution. The Go equivalent of the Java ApprovalWorkflow lab. The
// Workflow blocks in workflow.Await until an approve/reject Signal arrives, so a
// Query and an Update run against a live execution.
//
// The Workflow definition lives here so both standalone commands can import it:
// ./worker (registers + polls) and ./starter (starts a run).
package approval

import (
	"errors"
	"strings"

	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue  = "approval"
	WorkflowID = "approval-demo"
)

// ApprovalWorkflow waits for an approve/reject Signal while exposing its state
// via a Query and accepting a validated changeNote Update.
func ApprovalWorkflow(ctx workflow.Context, requestID string) (string, error) {
	status := "WAITING"
	note := "initial request"
	state := func() string {
		return requestID + " " + status + " (" + note + ")"
	}

	if err := workflow.SetQueryHandler(ctx, "currentState", func() (string, error) {
		return state(), nil
	}); err != nil {
		return "", err
	}

	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"changeNote",
		func(ctx workflow.Context, newNote string) (string, error) {
			note = newNote
			return state(), nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, newNote string) error {
				if strings.TrimSpace(newNote) == "" {
					return errors.New("note is required")
				}
				return nil
			},
		},
	)
	if err != nil {
		return "", err
	}

	approve := workflow.GetSignalChannel(ctx, "approve")
	reject := workflow.GetSignalChannel(ctx, "reject")
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(approve, func(c workflow.ReceiveChannel, _ bool) {
			var approver string
			c.Receive(ctx, &approver)
			status = "APPROVED by " + approver
		})
		selector.AddReceive(reject, func(c workflow.ReceiveChannel, _ bool) {
			var reason string
			c.Receive(ctx, &reason)
			status = "REJECTED: " + reason
		})
		selector.Select(ctx)
	})

	_ = workflow.Await(ctx, func() bool {
		return strings.HasPrefix(status, "APPROVED") || strings.HasPrefix(status, "REJECTED")
	})
	return state(), nil
}
