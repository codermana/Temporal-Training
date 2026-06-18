// Package tracing holds the Distributed Tracing lab: a three-step order
// pipeline whose client, Workflow, and Activity spans land in one Jaeger trace.
//
// Nothing here is tracing-specific — the spans come entirely from the
// OpenTelemetry interceptor wired in telemetry.go and attached in ./worker and
// ./starter. The Go equivalent of the Java OrderWorkflow lab.
package tracing

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "distributed-tracing"

// ValidateOrder, ChargePayment, ShipOrder are Activities. Each sleeps briefly so
// its span has a visible, distinct duration in the Jaeger timeline.
func ValidateOrder(ctx context.Context, orderID string) error {
	time.Sleep(200 * time.Millisecond)
	return nil
}

func ChargePayment(ctx context.Context, orderID string) (string, error) {
	time.Sleep(500 * time.Millisecond)
	return "pay-" + orderID, nil
}

func ShipOrder(ctx context.Context, orderID string) (string, error) {
	time.Sleep(300 * time.Millisecond)
	return "trk-" + orderID, nil
}

// OrderWorkflow runs the three Activities in sequence. Each ExecuteActivity call
// becomes a child span under the Workflow span.
func OrderWorkflow(ctx workflow.Context, orderID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	if err := workflow.ExecuteActivity(ctx, ValidateOrder, orderID).Get(ctx, nil); err != nil {
		return "", err
	}
	var paymentID string
	if err := workflow.ExecuteActivity(ctx, ChargePayment, orderID).Get(ctx, &paymentID); err != nil {
		return "", err
	}
	var tracking string
	if err := workflow.ExecuteActivity(ctx, ShipOrder, orderID).Get(ctx, &tracking); err != nil {
		return "", err
	}

	return fmt.Sprintf("order %s complete (payment=%s, tracking=%s)", orderID, paymentID, tracking), nil
}
