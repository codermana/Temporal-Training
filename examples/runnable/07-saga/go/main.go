// Command worker runs a long-lived Saga Worker on the "orders" queue, mirroring
// SagaWorker.java. It stays alive so you can start Workflows from the CLI.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
//
// Then, in another terminal:
//
//	temporal workflow start --task-queue orders --type OrderSagaWorkflow \
//	  --workflow-id order-OK --input '"order-1001"'
//
//	temporal workflow start --task-queue orders --type OrderSagaWorkflow \
//	  --workflow-id order-fail --input '"fail-at-ship"'
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(OrderSagaWorkflow)
	w.RegisterActivity(AuthorizePayment)
	w.RegisterActivity(ReserveInventory)
	w.RegisterActivity(Ship)
	w.RegisterActivity(CancelPayment)
	w.RegisterActivity(RestoreInventory)
	w.RegisterActivity(SendFailureNotification)

	log.Printf("Saga Worker polling task queue '%s'. Ctrl+C to stop.", TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
