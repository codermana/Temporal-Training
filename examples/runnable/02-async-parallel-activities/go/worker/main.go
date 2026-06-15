// Command worker runs the order-pricing Worker (standalone). It registers the
// Workflow + Activity and polls the pricing Task Queue forever; start a run
// from another terminal with `go run ./starter`.
//
//	go run ./worker        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/parallel"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, parallel.TaskQueue, worker.Options{})
	w.RegisterWorkflow(parallel.OrderPricingWorkflow)
	w.RegisterActivity(parallel.Price)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", parallel.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
