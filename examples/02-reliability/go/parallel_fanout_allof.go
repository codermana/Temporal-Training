package reliability

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Launch one Activity per partition, collect the Futures, then Get each to sum
// the counts. Collecting before getting is what makes them run in parallel.
func ProcessPartitions(ctx workflow.Context, partitions []int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
	})

	futures := make([]workflow.Future, 0, len(partitions))
	for _, partition := range partitions {
		futures = append(futures, workflow.ExecuteActivity(ctx, "ProcessPartition", partition))
	}

	total := 0
	for _, f := range futures {
		var count int
		if err := f.Get(ctx, &count); err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}
