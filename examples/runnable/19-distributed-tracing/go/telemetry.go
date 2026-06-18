package tracing

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

// otlpEndpoint is Jaeger's OTLP/gRPC receiver. Override with OTEL_EXPORTER_OTLP_ENDPOINT.
func otlpEndpoint() string {
	if v := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); v != "" {
		return v
	}
	return "localhost:4317"
}

// NewTracingInterceptor sets up an OpenTelemetry SDK exporting OTLP/gRPC to
// Jaeger, registers it as the global TracerProvider, and returns the Temporal
// tracing interceptor (attach it to client.Options.Interceptors) plus a
// shutdown func that flushes pending spans — call it before the process exits.
//
// Go has a native OpenTelemetry interceptor (go.temporal.io/sdk/contrib/
// opentelemetry), so there is no OpenTracing shim here, unlike the Java lab. The
// legacy alternative is go.temporal.io/sdk/contrib/opentracing — see the README.
func NewTracingInterceptor(ctx context.Context, serviceName string) (interceptor.ClientInterceptor, func(context.Context) error, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otlpEndpoint()),
	)
	if err != nil {
		return nil, nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(
			attribute.String("service.name", serviceName),
		)),
	)
	otel.SetTracerProvider(tp)

	// Critical: the interceptor propagates span context across the client ->
	// Workflow -> Activity boundaries using the global TextMapPropagator, whose
	// default is a no-op. Without W3C TraceContext set here every span becomes
	// its own root and the trace fragments instead of nesting into one.
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tracingInterceptor, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		Tracer: tp.Tracer(serviceName),
	})
	if err != nil {
		return nil, nil, err
	}

	return tracingInterceptor, tp.Shutdown, nil
}
