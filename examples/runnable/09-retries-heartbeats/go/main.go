// Command worker runs the Worker and kicks off one ProcessingWorkflow, mirroring
// RetriesWorker.java.
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
	w.RegisterWorkflow(ProcessingWorkflow)
	w.RegisterActivity(&FlakyActivities{})
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "retries-heartbeats-demo", TaskQueue: TaskQueue},
		ProcessingWorkflow,
		"order-42",
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Result: %s", result)
	log.Println("Open the Web UI and look for two ActivityTaskFailed events before ChargeCard succeeds,")
	log.Println("and the heartbeats recorded on ExportLargeReport.")
}
