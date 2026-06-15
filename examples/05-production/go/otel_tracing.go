package production

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
)

// OpenTelemetry span propagation. One ClientInterceptor on client.Options and the
// matching WorkerInterceptor on worker.Options carry spans through Workflow and
// Activity executions, so a single trace spans the whole run. Unlike Java (two
// distinct OpenTracing interceptors) the otel contrib builds one tracing
// interceptor that satisfies both roles:
//
//	import "go.temporal.io/sdk/contrib/opentelemetry"
//	ti, _ := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
//
// We accept it as a parameter so this snippet builds against the bare SDK.
func NewTracedClient(ti interceptor.ClientInterceptor) (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:     "127.0.0.1:7233",
		Interceptors: []interceptor.ClientInterceptor{ti},
	})
}

func NewTracedWorker(c client.Client, ti interceptor.WorkerInterceptor) worker.Worker {
	// The same tracing interceptor type continues the trace into Activities.
	return worker.New(c, "traced", worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{ti},
	})
}
