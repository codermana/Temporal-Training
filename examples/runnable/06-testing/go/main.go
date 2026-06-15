// Command worker runs the reminder Workflows against a real dev server — handy
// for capturing a history. The lab itself is exercised by reminder_test.go, which
// needs no server. Run the worker with:
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
package main

import (
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
	w.RegisterWorkflow(ReminderWorkflow)
	w.RegisterWorkflow(EmailReminderWorkflow)
	w.RegisterActivity(LookupEmail)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
}
