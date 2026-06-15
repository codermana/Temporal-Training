// Command worker runs the import Worker, mirroring WorkerMain.java. Env-driven so
// the same image serves any Task Queue.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
//
// Env: TEMPORAL_ADDRESS (default 127.0.0.1:7233), TEMPORAL_NAMESPACE (default),
// TASK_QUEUE (transform).
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	target := getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233")
	namespace := getenv("TEMPORAL_NAMESPACE", "default")
	taskQueue := getenv("TASK_QUEUE", TaskQueue)

	c, err := client.Dial(client.Options{HostPort: target, Namespace: namespace})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(ImportWorkflow)
	w.RegisterActivity(Validate)
	w.RegisterActivity(Transform)
	w.RegisterActivity(Load)

	if err := w.Start(); err != nil {
		log.Fatalln("unable to start worker:", err)
	}
	defer w.Stop()
	log.Printf("Worker started. target=%s namespace=%s taskQueue=%s", target, namespace, taskQueue)

	// Block until SIGTERM (docker stop / pod delete); w.Stop drains gracefully.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("Shutting down worker...")
}
