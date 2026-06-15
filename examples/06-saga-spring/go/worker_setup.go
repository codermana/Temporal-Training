package saga

import (
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Spring Boot autoconfig is a Java-only concept. The idiomatic Go analogue is a
// plain service main: dial the client, build a worker, register workflows and
// activities, and Run. There is no DI container — wiring is explicit. (This is the
// Go equivalent of spring_temporal_config.java's bean wiring.)
//
// Shown as a function rather than func main so the snippet builds as package saga.
func RunWorker(activities any) error {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		return err
	}
	defer c.Close()

	w := worker.New(c, "orders", worker.Options{})
	w.RegisterWorkflow(OrderSagaWorkflow)
	w.RegisterActivity(activities)

	log.Println("worker polling task queue 'orders'")
	return w.Run(worker.InterruptCh())
}
