// Package continueasnew holds the continue-as-new counter lab: process work in
// bounded batches per run. The Go equivalent of the Java CounterWorkflow lab.
// Each run processes a small batch, then returns a ContinueAsNew error to start
// a fresh run with a clean history — carrying forward only what the next run needs.
//
// The Workflow definition lives here so both standalone commands can import it:
// ./worker (registers + polls) and ./starter (starts a run).
package continueasnew

import (
	"strconv"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "continue-as-new"

// Cap the work done in a single run so history stays small...
const batchPerRun = 3

// ...and stop continuing once the whole job is done.
const total = 9

// CounterWorkflow processes up to batchPerRun items, then either completes or
// continues-as-new carrying processedSoFar forward.
func CounterWorkflow(ctx workflow.Context, processedSoFar int) (string, error) {
	logger := workflow.GetLogger(ctx)
	processed := processedSoFar
	thisRun := 0

	for thisRun < batchPerRun && processed < total {
		// Durable timer (never sleep on the wall clock in workflow code).
		_ = workflow.Sleep(ctx, 500*time.Millisecond)
		processed++
		thisRun++
		logger.Info("processed", "processed", processed, "total", total, "thisRun", thisRun)
	}

	if processed >= total {
		return "completed after " + strconv.Itoa(processed) + " iterations", nil
	}

	// Returning this error replaces the current run with a fresh one carrying only
	// processed forward.
	return "", workflow.NewContinueAsNewError(ctx, CounterWorkflow, processed)
}
