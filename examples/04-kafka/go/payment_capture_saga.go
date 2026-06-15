package kafka

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Driven by a `payment-authorized` Kafka event (bridged in via SignalWithStart).
// Capture the funds, then publish a settlement event. If publishing fails after
// the money moved, run a compensating refund so the system never ends in a
// "captured but never reported" state — a Kafka-fed saga.
func PaymentCaptureWorkflow(ctx workflow.Context, paymentID string, amountCents int64) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 4},
	})

	var captureRef string
	if err := workflow.ExecuteActivity(ctx, "Capture", paymentID, amountCents).Get(ctx, &captureRef); err != nil {
		return err
	}

	publishErr := workflow.ExecuteActivity(
		ctx, "PublishSettlement", "payment-settlements", paymentID, captureRef).Get(ctx, nil)
	if publishErr != nil {
		_ = workflow.ExecuteActivity(ctx, "Refund", paymentID, captureRef).Get(ctx, nil)
		return temporal.NewApplicationError(
			"settlement publish failed, refunded "+paymentID, "SettlementPublishFailed")
	}
	return nil
}
