// Command worker runs the Distributed Tracing Worker (standalone) with the
// OpenTelemetry interceptor attached. The interceptor continues the trace
// started on the client into the Workflow and each Activity, so the spans land
// in one Jaeger trace.
//
//	go run ./worker    // needs Temporal on 127.0.0.1:7233 and Jaeger OTLP on :4317
package main

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"training.temporal/tracing"
)

func main() {
	ctx := context.Background()

	tracingInterceptor, shutdown, err := tracing.NewTracingInterceptor(ctx, "temporal-worker-go")
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

	w := worker.New(c, tracing.TaskQueue, worker.Options{})
	w.RegisterWorkflow(tracing.OrderWorkflow)
	w.RegisterActivity(tracing.ValidateOrder)
	w.RegisterActivity(tracing.ChargePayment)
	w.RegisterActivity(tracing.ShipOrder)

	log.Printf("Tracing Worker started on task queue %q. Traces -> http://localhost:16686. Ctrl-C to stop.", tracing.TaskQueue)
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("worker stopped:", err)
	}
}
