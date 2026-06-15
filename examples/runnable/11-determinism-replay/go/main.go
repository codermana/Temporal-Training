// Command worker runs the data pipeline Workflow against a real dev server —
// handy for generating a history by hand. The lab's actual replay assertions live
// in replay_test.go, which spins up its own in-process dev server. Run with:
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
	w.RegisterWorkflowWithOptions(DataPipelineWorkflow, registerOpts)
	w.RegisterActivity(Extract)
	w.RegisterActivity(Load)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	run, err := c.ExecuteWorkflow(context.Background(),
		client.StartWorkflowOptions{ID: "pipeline-demo", TaskQueue: TaskQueue},
		WorkflowName)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}
	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Pipeline result: %s", result)
}
