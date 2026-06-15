package reliability

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

func TimeWorkflow(ctx workflow.Context) error {
	// Replay-safe timer. Records a TimerStarted event and frees the worker
	// goroutine instead of really sleeping. Never use time.Sleep in a Workflow.
	_ = workflow.Sleep(ctx, 6*time.Hour)

	// Replay-safe time source. Never call time.Now() in a Workflow.
	deadline := workflow.Now(ctx).Add(24 * time.Hour)
	_ = deadline
	return nil
}
