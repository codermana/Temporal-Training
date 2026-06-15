// Command worker runs the Worker and kicks off one BatchWorkflow, mirroring
// ChildWorker.java.
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
	// Parent and child run on the same Worker here; in production they can poll
	// different queues.
	w.RegisterWorkflow(BatchWorkflow)
	w.RegisterWorkflow(ItemWorkflow)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "batch-parent-demo", TaskQueue: TaskQueue},
		BatchWorkflow,
		[]string{"A", "B", "C"},
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Parent result:\n%s", result)
	log.Println("In the Web UI you'll see one parent execution plus child executions item-A / item-B / item-C.")
}
