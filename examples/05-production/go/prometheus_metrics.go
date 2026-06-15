package production

import (
	"go.temporal.io/sdk/client"
)

// Prometheus metrics in Go. The SDK emits metrics through a client.MetricsHandler
// set on client.Options (the analogue of Java attaching a metrics Scope to the
// service stubs). The production handler is built from the tally contrib module:
//
//	import (
//	    prom "github.com/uber-go/tally/v4/prometheus"
//	    sdktally "go.temporal.io/sdk/contrib/tally"
//	    "github.com/uber-go/tally/v4"
//	)
//	reporter, _ := prom.NewReporter(prom.Configuration{})
//	scope, _ := tally.NewRootScope(tally.ScopeOptions{
//	    CachedReporter: reporter, Separator: prom.DefaultSeparator}, time.Second)
//	handler := sdktally.NewMetricsHandler(scope)
//
// then pass it in. We accept the handler as a parameter here so this snippet
// stays buildable against the bare SDK.
func NewMetricsClient(handler client.MetricsHandler) (client.Client, error) {
	return client.Dial(client.Options{
		HostPort:       "127.0.0.1:7233",
		MetricsHandler: handler,
	})
}
