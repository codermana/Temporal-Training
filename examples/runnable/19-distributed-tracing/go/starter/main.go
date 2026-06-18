// Command starter starts one OrderWorkflow and prints its result (standalone
// client). The OpenTelemetry interceptor opens the root span and propagates its
// context to the server, so the Worker's spans nest under it. Run the Worker
// first with `go run ./worker` in another terminal.
//
//	go run ./starter [orderID]   // needs Temporal on :7233 and Jaeger OTLP on :4317
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"

	"training.temporal/tracing"
)

func main() {
	ctx := context.Background()

	tracingInterceptor, shutdown, err := tracing.NewTracingInterceptor(ctx, "temporal-client-go")
	if err != nil {
		log.Fatalln("unable to set up tracing:", err)
	}
	defer func() { _ = shutdown(ctx) }()

	c, err := client.Dial(client.Options{
		HostPort:     "127.0.0.1:7233",
		Interceptors: []interceptor.ClientInterceptor{tracingInterceptor},
	})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	orderID := "A-1001"
	if len(os.Args) > 1 {
		orderID = os.Args[1]
	}

	run, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{ID: "order-" + orderID, TaskQueue: tracing.TaskQueue},
		tracing.OrderWorkflow,
		orderID,
	)
	if err != nil {
		log.Fatalln("unable to start workflow:", err)
	}

	var result string
	if err := run.Get(ctx, &result); err != nil {
		log.Fatalln("workflow failed:", err)
	}
	log.Println(result)
	log.Println("Open the trace in Jaeger: http://localhost:16686 (service temporal-client-go)")
}
