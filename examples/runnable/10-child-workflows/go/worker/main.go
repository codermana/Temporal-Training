// Command worker runs the child-workflows Worker (standalone). It registers the
// parent + child Workflows and polls the child-workflows Task Queue forever;
// start a run from another terminal with `go run ./starter`.
//
//	go run ./worker        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/child"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, child.TaskQueue, worker.Options{})
	// Parent and child run on the same Worker here; in production they can poll
	// different queues.
	w.RegisterWorkflow(child.BatchWorkflow)
	w.RegisterWorkflow(child.ItemWorkflow)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", child.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
