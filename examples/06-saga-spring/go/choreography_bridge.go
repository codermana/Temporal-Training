package saga

import (
	"context"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

type DomainEvent struct {
	OrderID string
	Type    string
	Payload string
}

func OrderProcessWorkflow(ctx workflow.Context, orderID string) error {
	activityOptions := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	events := []DomainEvent{}
	shipmentFailed := false
	signalChan := workflow.GetSignalChannel(ctx, "onEvent")

	receiveEvent := func() {
		var event DomainEvent
		signalChan.Receive(ctx, &event)
		events = append(events, event)
		if event.Type == "ShipmentFailed" {
			shipmentFailed = true
		}
	}

	hasEvent := func(eventType string) bool {
		for _, event := range events {
			if event.Type == eventType {
				return true
			}
		}
		return false
	}

	for !hasEvent("OrderPlaced") {
		receiveEvent()
	}
	if err := workflow.ExecuteActivity(ctx, "ReserveInventory", orderID).Get(ctx, nil); err != nil {
		return err
	}

	for !hasEvent("PaymentCaptured") && !shipmentFailed {
		receiveEvent()
	}
	if shipmentFailed {
		return workflow.ExecuteActivity(ctx, "ReleaseInventory", orderID).Get(ctx, nil)
	}

	return workflow.ExecuteActivity(ctx, "RequestShipment", orderID).Get(ctx, nil)
}

func OnDomainEvent(ctx context.Context, c client.Client, event DomainEvent) error {
	// Choreography boundary: Kafka carries facts between services. This handler
	// only admits the event into Temporal; the Workflow owns order-local state.
	_, err := c.SignalWithStartWorkflow(
		ctx,
		"order-"+event.OrderID,
		"onEvent",
		event,
		client.StartWorkflowOptions{ID: "order-" + event.OrderID, TaskQueue: "orders"},
		OrderProcessWorkflow,
		event.OrderID,
	)
	return err
}
