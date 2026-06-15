package kafka

import (
	"context"
	"encoding/json"
)

// Order is the business row written by the Activity.
type Order struct {
	ID string
}

// WriteOrderAndOutbox writes the business row and an outbox row in ONE database
// transaction, so the row and its event can never diverge. A separate relay (or
// Debezium CDC) ships the outbox row to Kafka later — the Activity itself does
// no Kafka I/O, just an atomic local commit.
func WriteOrderAndOutbox(ctx context.Context, order Order) error {
	tx, err := db.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := orderRepository.Save(tx, order); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]string{"type": "OrderAccepted", "id": order.ID})
	if err := outboxRepository.Save(tx, "order-events", order.ID, payload); err != nil {
		return err
	}
	return tx.Commit()
}
