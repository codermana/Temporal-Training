// Command starter starts one OrderPricingWorkflow and prints its result
// (standalone client). Run the Worker first with `go run ./worker` in another
// terminal.
//
//	go run ./starter        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/parallel"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "order-pricing-demo", TaskQueue: parallel.TaskQueue},
		parallel.OrderPricingWorkflow,
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
