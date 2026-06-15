package foundations

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

func ReplaySafeWorkflow(ctx workflow.Context, batchDate string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})

	// Temporal records these deterministic decisions in Workflow history.
	now := workflow.Now(ctx) // replay-safe time source

	// Replay-safe randomness: use a side effect so the value is recorded once.
	var shard int
	_ = workflow.SideEffect(ctx, func(workflow.Context) any {
		return rand10()
	}).Get(&shard)

	// I/O belongs in Activities because Activity results are recorded in history.
	uri := fmt.Sprintf("s3://bucket/clean/%s/shard=%d?ts=%d", batchDate, shard, now.Unix())
	return workflow.ExecuteActivity(ctx, "Load", uri).Get(ctx, nil)
}
