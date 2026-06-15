// Command starter starts one WordCountWorkflow and prints its result.
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"training.temporal/wordcount"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	run, err := c.ExecuteWorkflow(
		context.Background(),
		client.StartWorkflowOptions{ID: "word-count-demo", TaskQueue: wordcount.TaskQueue},
		wordcount.WordCountWorkflow,
		wordcount.SampleChunks,
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var total int
	if err := run.Get(context.Background(), &total); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Printf("Total words: %d", total)
}
