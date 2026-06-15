package production

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Manual Worker sizing. Go has no thread-per-activity model (it uses goroutines),
// so the concurrency knobs are slot counts on worker.Options — the direct
// analogue of Java's setMaxConcurrentActivityExecutionSize / WorkflowTask sizes.
func StartManuallySizedWorker(c client.Client) worker.Worker {
	w := worker.New(c, "io-heavy", worker.Options{
		MaxConcurrentActivityExecutionSize:     200,
		MaxConcurrentWorkflowTaskExecutionSize: 20,
	})
	w.RegisterWorkflow(IoWorkflow)
	w.RegisterActivity(IoActivity)
	return w
}

func IoWorkflow(ctx workflow.Context) error { return nil }

func IoActivity(ctx context.Context) error { return nil }
