package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/client"
)

// SignalWithStartWorkflow is the Go equivalent of Java's signalWithStart: it
// starts the Workflow on the first event for a key and signals the existing
// execution for every event after that. A fixed WorkflowID derived from the
// record key means the same key always maps to the same execution.
func OnKafkaRecord(ctx context.Context, c client.Client, record kafka.Message) error {
	orderID := string(record.Key)
	_, err := c.SignalWithStartWorkflow(
		ctx,
		"order-"+orderID,     // WorkflowID
		"orderEvent",         // signal name
		string(record.Value), // signal arg
		client.StartWorkflowOptions{ID: "order-" + orderID, TaskQueue: "orders"},
		"OrderWorkflow", // workflow type
		orderID,         // workflow arg
	)
	return err
}
