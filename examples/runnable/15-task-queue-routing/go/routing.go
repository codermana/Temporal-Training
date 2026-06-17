// Package routing holds the task-queue-routing demo: one Workflow whose two
// Activities are routed to different Worker pools by Task Queue. The unit of
// "who registers what" is the Task Queue, not the Worker - no single Worker
// registers the full set.
package routing

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Each name maps to one Worker pool that registers only its subset of work.
const (
	TaskQueueOrders   = "orders"
	TaskQueuePayments = "payments"
	TaskQueueMedia    = "media"
)

// Charge is an Activity served by the payments pool.
func Charge(ctx context.Context, orderID string) (string, error) {
	// The log line proves which pool executed this Activity.
	fmt.Printf("[payments pool] charging order %s\n", orderID)
	return "charged $42.00", nil
}

// Render is an Activity served by the media/GPU pool.
func Render(ctx context.Context, orderID string) (string, error) {
	fmt.Printf("[media pool] rendering receipt for order %s\n", orderID)
	return "s3://receipts/" + orderID + ".pdf", nil
}

// OrderWorkflow runs on the orders queue; each Activity call names the Task
// Queue whose pool registers that Activity.
func OrderWorkflow(ctx workflow.Context, orderID string) (string, error) {
	var charged string
	payCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           TaskQueuePayments,
		StartToCloseTimeout: 30 * time.Second,
	})
	if err := workflow.ExecuteActivity(payCtx, Charge, orderID).Get(payCtx, &charged); err != nil {
		return "", err
	}

	var receipt string
	mediaCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           TaskQueueMedia,
		StartToCloseTimeout: 5 * time.Minute,
	})
	if err := workflow.ExecuteActivity(mediaCtx, Render, orderID).Get(mediaCtx, &receipt); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s: %s / %s", orderID, charged, receipt), nil
}
