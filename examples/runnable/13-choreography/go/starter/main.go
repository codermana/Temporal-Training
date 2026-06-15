package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"

	"training.temporal/choreography"
)

func main() {
	ctx := context.Background()
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	stream := []choreography.DomainEvent{
		{OrderID: "choreo-1001", Type: "OrderPlaced", Payload: "cart=3 items"},
		{OrderID: "choreo-1001", Type: "PaymentCaptured", Payload: "paymentId=pay-777"},
		{OrderID: "choreo-1001", Type: "InventoryReserved", Payload: "reservation=inv-42"},
	}

	for _, event := range stream {
		if err := signalWithStart(ctx, c, event); err != nil {
			log.Fatalln("unable to admit event:", err)
		}
		fmt.Printf("accepted event %s for %s\n", event.Type, event.OrderID)
		time.Sleep(750 * time.Millisecond)
	}

	value, err := c.QueryWorkflow(ctx, choreography.WorkflowID, "", "status")
	if err != nil {
		log.Fatalln("unable to query workflow:", err)
	}
	var status string
	if err := value.Get(&status); err != nil {
		log.Fatalln("unable to query workflow:", err)
	}
	fmt.Println("final status:", status)
}

func signalWithStart(ctx context.Context, c client.Client, event choreography.DomainEvent) error {
	_, err := c.SignalWithStartWorkflow(
		ctx,
		choreography.WorkflowID,
		"onEvent",
		event,
		client.StartWorkflowOptions{
			ID:        choreography.WorkflowID,
			TaskQueue: choreography.TaskQueue,
		},
		choreography.OrderProcessWorkflow,
		event.OrderID,
	)
	return err
}
