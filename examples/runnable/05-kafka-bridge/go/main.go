// Command worker runs the Temporal Worker on the "orders" Task Queue plus a
// Kafka signal bridge, mirroring KafkaWorker.java.
//
//	go run .        // needs a Temporal dev server + a Kafka broker
//
// The bridge is a plain Kafka consumer (NOT Workflow code). It commits offsets
// only after SignalWithStartWorkflow returns — at-least-once into Temporal.
package main

import (
	"context"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	bootstrapServers := env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	inputTopic := env("KAFKA_ORDERS_TOPIC", "orders")
	outcomeTopic := env("KAFKA_OUTCOMES_TOPIC", "order-outcomes")

	c, err := client.Dial(client.Options{HostPort: env("TEMPORAL_ADDRESS", "127.0.0.1:7233")})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	activities := NewOutcomeActivities(bootstrapServers, outcomeTopic)

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(OrderWorkflow)
	w.RegisterActivity(activities.PublishOutcome)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	log.Printf("Kafka bridge running. brokers=%s in=%s out=%s taskQueue=%s. Ctrl+C to stop.",
		bootstrapServers, inputTopic, outcomeTopic, TaskQueue)
	runBridge(context.Background(), c, bootstrapServers, inputTopic)
}

// runBridge consumes the orders topic and SignalWithStarts one Workflow per key.
func runBridge(ctx context.Context, c client.Client, bootstrapServers, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{bootstrapServers},
		Topic:       topic,
		GroupID:     "temporal-order-bridge",
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Fatalln("consumer error:", err)
		}
		orderID := string(msg.Key)
		// SignalWithStartWorkflow == Java signalWithStart: start on the first
		// event for a key, signal the existing run for later events.
		_, err = c.SignalWithStartWorkflow(
			ctx,
			"order-"+orderID,
			"orderEvent",
			string(msg.Value),
			client.StartWorkflowOptions{ID: "order-" + orderID, TaskQueue: TaskQueue},
			OrderWorkflow,
			orderID,
		)
		if err != nil {
			log.Fatalln("signalWithStart failed:", err)
		}
		// Commit ONLY after the signal lands.
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Fatalln("commit failed:", err)
		}
	}
}
