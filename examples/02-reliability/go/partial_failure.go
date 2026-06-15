package reliability

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// Let some partitions fail without aborting the whole fan-out. We start every
// Activity, then Get each Future and record a per-partition status — a failed
// Get returns an error here instead of unwinding the Workflow.
func Process(ctx workflow.Context, partitions []int) (map[int]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	futures := make(map[int]workflow.Future, len(partitions))
	for _, partition := range partitions {
		futures[partition] = workflow.ExecuteActivity(ctx, "ProcessWithStatus", partition)
	}

	result := make(map[int]string, len(partitions))
	for partition, f := range futures {
		var status string
		if err := f.Get(ctx, &status); err != nil {
			result[partition] = fmt.Sprintf("FAILED: %v", err)
		} else {
			result[partition] = status
		}
	}
	return result, nil
}
