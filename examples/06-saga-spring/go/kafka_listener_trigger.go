package saga

import (
	"context"

	"go.temporal.io/sdk/client"
)

// Spring's @KafkaListener is Java-only; the idiomatic Go analogue is a plain
// consumer loop whose handler calls the Temporal client. The trigger contains no
// saga logic — it just starts the workflow.
//
// SignalWithStartWorkflow keeps Kafka redeliveries idempotent: the workflow is
// started once per orderID, and later events for the same key signal the
// already-running execution instead of crashing.
func OnOrder(ctx context.Context, c client.Client, request OrderRequest) error {
	_, err := c.SignalWithStartWorkflow(
		ctx,
		"order-"+request.OrderID,
		"onUpdate",
		request,
		client.StartWorkflowOptions{ID: "order-" + request.OrderID, TaskQueue: "orders"},
		OrderSagaWorkflow,
		request.OrderID,
	)
	return err
}

// Consume is pseudocode for the consumer loop (kafka-go / sarama in a real app):
//
//	for msg := range reader.Messages() {
//	    _ = OnOrder(ctx, c, decode(msg.Value))
//	}
func Consume(ctx context.Context, c client.Client) {}
