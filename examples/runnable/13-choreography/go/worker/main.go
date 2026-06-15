package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/choreography"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, choreography.TaskQueue, worker.Options{})
	w.RegisterWorkflow(choreography.OrderProcessWorkflow)
	w.RegisterActivity(choreography.ReserveLocalInventory)
	w.RegisterActivity(choreography.RequestShipment)
	w.RegisterActivity(choreography.ReleaseInventory)

	log.Printf("Worker started on task queue %q. Ctrl-C to stop.", choreography.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}

