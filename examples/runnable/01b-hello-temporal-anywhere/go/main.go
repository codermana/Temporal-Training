// Command main runs the env-driven Hello lab (Docker 1.2b / Cloud 1.2c).
//
//	go run .
//	# local:  nothing to set (defaults to 127.0.0.1:7233)
//	# cloud:  TEMPORAL_ADDRESS=... TEMPORAL_NAMESPACE=... TEMPORAL_API_KEY=... go run .
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := dialFromEnv()
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflow(GreetingWorkflow)
	w.RegisterActivity(ComposeGreeting)
	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()

	name := getenv("GREET_NAME", "Ada")
	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "hello-anywhere-demo", TaskQueue: TaskQueue},
		GreetingWorkflow,
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
