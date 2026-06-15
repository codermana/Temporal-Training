// Package main holds the order saga lab: authorize payment -> reserve inventory
// -> ship, with compensation. The Go equivalent of the Java OrderSagaWorkflow lab.
// Go has no built-in Saga helper; the idiomatic pattern is a slice of compensation
// closures that we run in reverse on error.
package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "orders"

// --- Forward steps ---

func AuthorizePayment(ctx context.Context, orderID string) (string, error) {
	return "payment-" + orderID, nil
}

func ReserveInventory(ctx context.Context, orderID string) (string, error) {
	return "reservation-" + orderID, nil
}

func Ship(ctx context.Context, orderID string) error {
	// Fail on demand so you can trigger compensation from the CLI.
	if strings.Contains(strings.ToLower(orderID), "fail") {
		return errors.New("shipping label service failed")
	}
	return nil
}

// --- Compensating steps (should be idempotent: they may be retried) ---

func CancelPayment(ctx context.Context, paymentID string) error {
	activity.GetLogger(ctx).Info("cancelled " + paymentID)
	return nil
}

func RestoreInventory(ctx context.Context, reservationID string) error {
	activity.GetLogger(ctx).Info("restored " + reservationID)
	return nil
}

func SendFailureNotification(ctx context.Context, orderID, reason string) error {
	activity.GetLogger(ctx).Info("order " + orderID + " failed: " + reason)
	return nil
}

// OrderSagaWorkflow runs the forward steps and, on failure, unwinds in reverse.
func OrderSagaWorkflow(ctx workflow.Context, orderID string) (string, error) {
	// Bound the retries so a permanent failure exhausts quickly and we reach
	// compensation. Without MaximumAttempts the default policy retries forever and
	// the saga never reaches the compensation path.
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 500 * time.Millisecond,
			MaximumAttempts: 3,
		},
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
	if err := workflow.ExecuteActivity(ctx, AuthorizePayment, orderID).Get(ctx, &paymentID); err != nil {
		return "FAILED", err
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, CancelPayment, paymentID).Get(ctx, nil)
	})

	var reservationID string
	if err := workflow.ExecuteActivity(ctx, ReserveInventory, orderID).Get(ctx, &reservationID); err != nil {
		return compensateAndNotify(ctx, compensate, orderID, err)
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, RestoreInventory, reservationID).Get(ctx, nil)
	})

	if err := workflow.ExecuteActivity(ctx, Ship, orderID).Get(ctx, nil); err != nil {
		return compensateAndNotify(ctx, compensate, orderID, err)
	}

	return "COMPLETED", nil
}

func compensateAndNotify(ctx workflow.Context, compensate func(), orderID string, cause error) (string, error) {
	compensate()
	_ = workflow.ExecuteActivity(ctx, SendFailureNotification, orderID, cause.Error()).Get(ctx, nil)
	return "COMPENSATED", nil
}
