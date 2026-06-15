package reliability

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Start a long-running export under a cancellable child context, then race it
// against a deadline timer with a Selector. When the deadline wins we call
// cancel(): that propagates to the Activity on its next heartbeat (its ctx.Err
// becomes canceled) so it can clean up partial work.
func ExportWithDeadline(ctx workflow.Context, tableName string, deadline time.Duration) (string, error) {
	actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
	})
	childCtx, cancel := workflow.WithCancel(actCtx)

	exportF := workflow.ExecuteActivity(childCtx, "ExportLargeTable", tableName)
	timer := workflow.NewTimer(ctx, deadline)

	var result string
	var finishedInTime bool
	workflow.NewSelector(ctx).
		AddFuture(exportF, func(f workflow.Future) {
			finishedInTime = true
			_ = f.Get(ctx, &result)
		}).
		AddFuture(timer, func(workflow.Future) { cancel() }).
		Select(ctx)

	if !finishedInTime {
		return "", temporal.NewApplicationError("export timed out", "ExportTimeout")
	}
	return result, nil
}
