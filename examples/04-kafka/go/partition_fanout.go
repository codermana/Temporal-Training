package kafka

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// PartitionRange is one contiguous block of partitions handled by one Activity.
type PartitionRange struct {
	Start int
	End   int
}

// Fan one Activity out per partition range, collect the Futures, then Get each
// to sum the counts. Keep the fan-out capped (a handful of ranges) for laptops
// and predictable Activity pressure — the equivalent of Promise.allOf over
// ranges.
func ProcessRanges(ctx workflow.Context) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 20 * time.Minute,
	})

	ranges := []PartitionRange{{0, 3}, {4, 7}, {8, 11}}
	futures := make([]workflow.Future, 0, len(ranges))
	for _, r := range ranges {
		futures = append(futures, workflow.ExecuteActivity(ctx, "ProcessRange", r))
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
