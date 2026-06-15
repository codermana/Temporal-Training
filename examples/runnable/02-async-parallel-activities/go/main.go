// Command worker runs the worker and kicks off one OrderPricingWorkflow,
// mirroring PricingWorker.java.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"context"
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
	w.RegisterWorkflow(OrderPricingWorkflow)
	w.RegisterActivity(Price)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "order-pricing-demo", TaskQueue: TaskQueue},
		OrderPricingWorkflow,
		[]string{"book", "lamp", "desk"},
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var total int
	if err := run.Get(context.Background(), &total); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Total price: %d", total)
}
