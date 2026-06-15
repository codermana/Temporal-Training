// Command worker runs the worker and kicks off one CounterWorkflow, mirroring
// ContinueAsNewWorker.java.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
//
// run.Get transparently follows the chain of continue-as-new runs to the final
// result, so the client sees a single Workflow ID with multiple chained Runs.
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
	w.RegisterWorkflow(CounterWorkflow)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "continue-as-new-demo", TaskQueue: TaskQueue},
		CounterWorkflow,
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
