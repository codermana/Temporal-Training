// Package main holds the Kafka -> Temporal -> Kafka order pipeline (Go port of
// the Java lab). One long-lived OrderWorkflow per order key, fed by Signals from
// a plain Kafka consumer "bridge". The Workflow publishes an outcome per event
// through an idempotent producer Activity. Workflow code never touches Kafka
// directly — all I/O lives in the bridge (consume) and the Activity (produce).
package main

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "orders"

// OutcomeActivities is the producer Activity. Idempotent + RequireAll acks so
// Temporal retries can't duplicate outcome events on the broker.
type OutcomeActivities struct {
	writer *kafka.Writer
}

func NewOutcomeActivities(bootstrapServers, topic string) *OutcomeActivities {
	return &OutcomeActivities{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(bootstrapServers),
			Topic:        topic,
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.Hash{},
		},
	}
}

// PublishOutcome blocks on the write so a broker failure returns an error and
// Temporal retries.
func (a *OutcomeActivities) PublishOutcome(ctx context.Context, orderID, outcome string) error {
	return a.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: []byte(outcome),
	})
}

// OrderWorkflow is one long-lived Workflow per order key. The first Kafka event
// starts it; every later event for the same key is delivered as a Signal to this
// same execution (via SignalWithStartWorkflow in the bridge). It stays open,
// publishing an outcome per event, and continue-as-news to bound its history.
func OrderWorkflow(ctx workflow.Context, orderID string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	eventCh := workflow.GetSignalChannel(ctx, "orderEvent")
	processed := 0
	var a *OutcomeActivities // method-set registration; nil receiver is fine here.

	for {
		var payload string
		eventCh.Receive(ctx, &payload) // blocks until the next signal arrives.
		if err := workflow.ExecuteActivity(
			ctx, a.PublishOutcome, orderID, "accepted:"+orderID+":"+payload).Get(ctx, nil); err != nil {
			return err
		}
		processed++
		if processed >= 1000 {
			return workflow.NewContinueAsNewError(ctx, OrderWorkflow, orderID)
		}
	}
}
