package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/activity"
)

// A polling consumer lives inside an Activity (blocking I/O, never Workflow
// code). We use ReadMessage (not auto-commit) and CommitMessages only after the
// Activity's work succeeds — giving at-least-once delivery into Temporal.
func PollBatch(ctx context.Context, topic string) ([]string, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       topic,
		GroupID:     "temporal-poll-batch",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	var values []string
	var batch []kafka.Message
	for len(values) < 100 {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			return nil, err
		}
		values = append(values, string(msg.Value))
		batch = append(batch, msg)
		// Heartbeat the offset so a retry resumes from here.
		activity.RecordHeartbeat(ctx, fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset))
	}

	// Commit only after Activity work succeeds.
	if err := reader.CommitMessages(ctx, batch...); err != nil {
		return nil, err
	}
	return values, nil
}
