// Command worker runs the env-driven Hello Worker (standalone). It registers the
// Workflow + Activity and polls the hello-anywhere Task Queue forever; start a run
// from another terminal with `go run ./starter`. The connection is built from
// environment variables, so the same command targets a local dev server, a
// Dockerized cluster (Lab 1.2b), or Temporal Cloud (Lab 1.2c).
//
//	go run ./worker        // local: defaults to 127.0.0.1:7233
//	# cloud: TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... go run ./worker
package main

import (
	"log"

	"go.temporal.io/sdk/worker"

	hello "training.temporal/hello-anywhere"
)

func main() {
	c, err := hello.DialFromEnv()
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, hello.TaskQueue, worker.Options{})
	w.RegisterWorkflow(hello.GreetingWorkflow)
	w.RegisterActivity(hello.ComposeGreeting)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", hello.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
