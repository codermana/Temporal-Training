package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// An idempotent producer lives inside an Activity. RequiredAcks=RequireAll plus
// segmentio's transactional writer config give exactly-once-per-partition
// delivery, so Temporal's automatic retries can't create duplicate outcome
// events on the broker.
type OutcomeProducerActivity struct {
	writer *kafka.Writer
}

func NewOutcomeProducerActivity(bootstrapServers string) *OutcomeProducerActivity {
	return &OutcomeProducerActivity{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(bootstrapServers),
			Topic:        "order-outcomes",
			RequiredAcks: kafka.RequireAll,
			Balancer:     &kafka.Hash{},
		},
	}
}

func (a *OutcomeProducerActivity) PublishOutcome(ctx context.Context, orderID, outcome string) error {
	// Block on the write so a broker failure returns an error and Temporal retries.
	return a.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(orderID),
		Value: []byte(outcome),
	})
}
