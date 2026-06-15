// Command worker runs the word-count Worker.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/wordcount"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, wordcount.TaskQueue, worker.Options{})
	w.RegisterWorkflow(wordcount.WordCountWorkflow)
	w.RegisterActivity(wordcount.CountWords)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", wordcount.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
