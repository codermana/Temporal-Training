// Command starter starts one GreetingWorkflow and prints its result
// (standalone client). Run the Worker first with `go run ./worker` in another
// terminal.
//
//	go run ./starter        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/hello"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "hello-temporal-demo", TaskQueue: hello.TaskQueue},
		hello.GreetingWorkflow,
		"Ada",
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var result string
	if err := run.Get(context.Background(), &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Println(result)
}
