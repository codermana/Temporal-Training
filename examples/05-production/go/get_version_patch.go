package production

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// reserve and charge are Activities registered elsewhere.

// VersionedWorkflow shows the Go equivalent of Java's Workflow.getVersion:
// workflow.GetVersion returns DefaultVersion when replaying histories recorded
// before the change, and the new version for fresh executions, so old runs keep
// their original command order.
func VersionedWorkflow(ctx workflow.Context, orderID string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	version := workflow.GetVersion(ctx, "charge-before-reserve", workflow.DefaultVersion, 1)
	if version == workflow.DefaultVersion {
		if err := workflow.ExecuteActivity(ctx, "Reserve", orderID).Get(ctx, nil); err != nil {
			return err
		}
		return workflow.ExecuteActivity(ctx, "Charge", orderID).Get(ctx, nil)
	}

	if err := workflow.ExecuteActivity(ctx, "Charge", orderID).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, "Reserve", orderID).Get(ctx, nil)
}
