// Command starter starts one GreetingWorkflow and prints its result (standalone
// client). Run the Worker first with `go run ./worker` in another terminal. The
// connection is built from the same environment variables as the Worker, so the
// starter targets a local dev server, a Dockerized cluster (Lab 1.2b), or
// Temporal Cloud (Lab 1.2c). Set GREET_NAME to change the greeting input.
//
//	go run ./starter        // local: defaults to 127.0.0.1:7233
//	# cloud: TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... go run ./starter
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	hello "training.temporal/hello-anywhere"
)

func main() {
	c, err := hello.DialFromEnv()
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	name := hello.Getenv("GREET_NAME", "Ada")
	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "hello-anywhere-demo", TaskQueue: hello.TaskQueue},
		hello.GreetingWorkflow,
		name,
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
