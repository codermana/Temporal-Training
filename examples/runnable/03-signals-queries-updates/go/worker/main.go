// Command worker runs the approval Worker (standalone). It registers the
// Workflow and polls the approval Task Queue forever; start a run from another
// terminal with `go run ./starter`.
//
//	go run ./worker        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/approval"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, approval.TaskQueue, worker.Options{})
	w.RegisterWorkflow(approval.ApprovalWorkflow)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", approval.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
