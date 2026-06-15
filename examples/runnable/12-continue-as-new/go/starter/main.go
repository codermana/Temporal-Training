// Command starter starts one CounterWorkflow and prints its result (standalone
// client). Run the Worker first with `go run ./worker` in another terminal.
//
//	go run ./starter        // needs a Temporal dev server on 127.0.0.1:7233
//
// run.Get transparently follows the chain of continue-as-new runs to the final
// result, so the client sees a single Workflow ID with multiple chained Runs.
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/continueasnew"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "continue-as-new-demo", TaskQueue: continueasnew.TaskQueue},
		continueasnew.CounterWorkflow,
		0,
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Result: %s", result)
	log.Println("In the Web UI, the single Workflow ID shows multiple Runs chained by ContinueAsNew.")
}
