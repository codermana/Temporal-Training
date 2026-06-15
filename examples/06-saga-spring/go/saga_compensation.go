package saga

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Go has no built-in Saga helper. The idiomatic pattern is a slice of
// compensation closures: each forward step appends its undo, and on error we run
// them in reverse. This mirrors Java's io.temporal.workflow.Saga semantics.

// OrderSagaWorkflow runs the forward steps and, on failure, unwinds in reverse.
func OrderSagaWorkflow(ctx workflow.Context, orderID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	// Each compensation is a closure capturing the id it needs to undo.
	var compensations []func()
	compensate := func() {
		// Reverse order: last registered runs first.
		for i := len(compensations) - 1; i >= 0; i-- {
			compensations[i]()
		}
	}

	var paymentID string
	if err := workflow.ExecuteActivity(ctx, "AuthorizePayment", orderID).Get(ctx, &paymentID); err != nil {
		return "FAILED", err
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, "CancelPayment", paymentID).Get(ctx, nil)
	})

	var reservationID string
	if err := workflow.ExecuteActivity(ctx, "ReserveInventory", orderID).Get(ctx, &reservationID); err != nil {
		compensate()
		_ = workflow.ExecuteActivity(ctx, "SendFailureNotification", orderID, err.Error()).Get(ctx, nil)
		return "COMPENSATED", nil
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, "RestoreInventory", reservationID).Get(ctx, nil)
	})

	if err := workflow.ExecuteActivity(ctx, "Ship", orderID).Get(ctx, nil); err != nil {
		compensate()
		_ = workflow.ExecuteActivity(ctx, "SendFailureNotification", orderID, err.Error()).Get(ctx, nil)
		return "COMPENSATED", nil
	}

	return "COMPLETED", nil
}
