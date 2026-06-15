// Command starter starts one BatchWorkflow (the parent) and prints its result
// (standalone client). Run the Worker first with `go run ./worker` in another
// terminal.
//
//	go run ./starter        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/child"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "batch-parent-demo", TaskQueue: child.TaskQueue},
		child.BatchWorkflow,
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
