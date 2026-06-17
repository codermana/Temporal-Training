// Command starter starts one OrderWorkflow and prints its summary. Run the
// worker first so the three pools are polling. Watch the worker terminal: the
// [payments pool] and [media pool] lines come from two different Workers, even
// though neither registered the full set of Activities.
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/routing"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "order-routing-demo", TaskQueue: routing.TaskQueueOrders},
		routing.OrderWorkflow,
		"order-1001",
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var summary string
	if err := run.Get(context.Background(), &summary); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Workflow result: %s", summary)
}
