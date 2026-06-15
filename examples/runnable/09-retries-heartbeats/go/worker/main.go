// Command worker runs the ProcessingWorkflow Worker (standalone). It registers
// the Workflow + Activities and polls the retries-heartbeats Task Queue forever;
// start a run from another terminal with `go run ./starter`.
//
//	go run ./worker        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/retries"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, retries.TaskQueue, worker.Options{})
	w.RegisterWorkflow(retries.ProcessingWorkflow)
	w.RegisterActivity(&retries.FlakyActivities{})

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", retries.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
