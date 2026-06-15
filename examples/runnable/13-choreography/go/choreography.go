package choreography

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue  = "choreography"
	WorkflowID = "order-choreo-1001"
)

type DomainEvent struct {
	OrderID string
	Type    string
	Payload string
}

type StatusQuery struct {
	OrderID string
	Status  string
	Events  int
}

func OrderProcessWorkflow(ctx workflow.Context, orderID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})

	events := []DomainEvent{}
	status := "WAITING_FOR_ORDER"
	failureReason := ""
	signalChan := workflow.GetSignalChannel(ctx, "onEvent")

	state := func() string {
		return fmt.Sprintf("%s %s events=%d", orderID, status, len(events))
	}

	if err := workflow.SetQueryHandler(ctx, "status", func() (string, error) {
		return state(), nil
	}); err != nil {
		return "", err
	}

	receiveEvent := func() {
		var event DomainEvent
		signalChan.Receive(ctx, &event)
		events = append(events, event)
		if event.Type == "PaymentFailed" {
			failureReason = event.Payload
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
	status = "ORDER_ACCEPTED"
	if err := workflow.ExecuteActivity(ctx, ReserveLocalInventory, orderID).Get(ctx, nil); err != nil {
		return "", err
	}

	for !hasEvent("PaymentCaptured") && !hasEvent("PaymentFailed") {
		receiveEvent()
	}
	if hasEvent("PaymentFailed") {
		status = "COMPENSATING_PAYMENT_FAILURE"
		if err := workflow.ExecuteActivity(ctx, ReleaseInventory, orderID, failureReason).Get(ctx, nil); err != nil {
			return "", err
		}
		status = "CANCELLED"
		return state(), nil
	}

	status = "PAID_WAITING_FOR_INVENTORY"
	for !hasEvent("InventoryReserved") {
		receiveEvent()
	}
	if err := workflow.ExecuteActivity(ctx, RequestShipment, orderID).Get(ctx, nil); err != nil {
		return "", err
	}

	status = "READY_TO_SHIP"
	return state(), nil
}

func ReserveLocalInventory(orderID string) error {
	fmt.Println("reserved local inventory for", orderID)
	return nil
}

func RequestShipment(orderID string) error {
	fmt.Println("requested shipment for", orderID)
	return nil
}

func ReleaseInventory(orderID string, reason string) error {
	fmt.Printf("released inventory for %s: %s\n", orderID, reason)
	return nil
}

