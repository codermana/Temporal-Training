// Command worker starts the three Worker pools this demo routes across. In
// production each pool is its own deployment; here one process hosts all three
// so the routing is easy to watch. Each worker registers only the SUBSET of
// work routed to its Task Queue.
package main

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"training.temporal/routing"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	// Orchestrator pool: the Workflow only, no Activities.
	orders := worker.New(c, routing.TaskQueueOrders, worker.Options{})
	orders.RegisterWorkflow(routing.OrderWorkflow)

	// Payments pool: ONLY the Charge Activity.
	payments := worker.New(c, routing.TaskQueuePayments, worker.Options{})
	payments.RegisterActivity(routing.Charge)

	// Media/GPU pool: ONLY the Render Activity.
	media := worker.New(c, routing.TaskQueueMedia, worker.Options{})
	media.RegisterActivity(routing.Render)

	// Start() is non-blocking; Run() blocks until interrupt.
	if err := orders.Start(); err != nil {
		log.Fatalln("unable to start orders worker:", err)
	}
	if err := payments.Start(); err != nil {
		log.Fatalln("unable to start payments worker:", err)
	}
	log.Println("Workers started: orders=[OrderWorkflow], payments=[Charge], media=[Render]. Ctrl-C to stop.")
	if err := media.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
