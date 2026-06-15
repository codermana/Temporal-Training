package production

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// Go has no JVM virtual threads (Java's virtual_threads.java). It doesn't need
// them: each Activity invocation already runs on its own goroutine, which is
// cheap and scheduled onto a small thread pool by the runtime — the same payoff
// virtual threads give the JVM, built into the language. So worker concurrency is
// just a slot count, not a thread-pool type. Size it via worker.Options.
func StartHighConcurrencyWorker(c client.Client) worker.Worker {
	return worker.New(c, "high-concurrency-activities", worker.Options{
		// Each in-flight activity is one goroutine; this caps how many run at once.
		MaxConcurrentActivityExecutionSize: 1000,
	})
}
