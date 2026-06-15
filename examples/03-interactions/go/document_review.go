package interactions

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Real-world human-in-the-loop: a document moves through review. Reviewers push
// comments via an Update (synchronous, validated, returns the running count)
// while the final publish/reject decision arrives as a Signal. The run method
// blocks on a single Await that either decision satisfies, with an escalation
// timer so a stalled review auto-rejects instead of hanging forever.
func DocumentReviewWorkflow(ctx workflow.Context, docID string) (string, error) {
	var comments []string
	decision := ""

	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"addComment",
		func(ctx workflow.Context, reviewer, comment string) (int, error) {
			comments = append(comments, reviewer+": "+comment)
			return len(comments), nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, reviewer, comment string) error {
				if strings.TrimSpace(comment) == "" {
					return errors.New("comment must not be empty")
				}
				return nil
			},
		},
	)
	if err != nil {
		return "", err
	}

	if err := workflow.SetQueryHandler(ctx, "pendingComments", func() ([]string, error) {
		return comments, nil
	}); err != nil {
		return "", err
	}

	publish := workflow.GetSignalChannel(ctx, "publish")
	reject := workflow.GetSignalChannel(ctx, "reject")
	workflow.Go(ctx, func(ctx workflow.Context) {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(publish, func(c workflow.ReceiveChannel, _ bool) {
			var approver string
			c.Receive(ctx, &approver)
			decision = "PUBLISHED by " + approver
		})
		selector.AddReceive(reject, func(c workflow.ReceiveChannel, _ bool) {
			var reason string
			c.Receive(ctx, &reason)
			decision = "REJECTED: " + reason
		})
		selector.Select(ctx)
	})

	// Auto-reject if no decision lands within the SLA window.
	if ok, err := workflow.AwaitWithTimeout(ctx, 3*24*time.Hour, func() bool {
		return decision != ""
	}); err != nil {
		return "", err
	} else if !ok {
		decision = "REJECTED: review SLA expired"
	}
	return fmt.Sprintf("%s -> %s (%d comments)", docID, decision, len(comments)), nil
}
