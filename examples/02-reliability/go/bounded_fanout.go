package reliability

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Real-world: fan out 10k notifications, but a downstream provider only
// tolerates ~20 in-flight calls. A deterministic in-Workflow counter gated by
// workflow.Await caps concurrency while keeping the pipeline full — the durable
// equivalent of a bounded worker pool. (Don't use a Go sync primitive here;
// workflow.Await is the replay-safe wait.)
func SendAll(ctx workflow.Context, userIDs []string, maxInFlight int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	inFlight := 0
	futures := make([]workflow.Future, 0, len(userIDs))
	for _, id := range userIDs {
		// Park the Workflow (deterministically) until a slot frees up.
		_ = workflow.Await(ctx, func() bool { return inFlight < maxInFlight })
		inFlight++
		f := workflow.ExecuteActivity(ctx, "Send", id)
		workflow.Go(ctx, func(ctx workflow.Context) {
			_ = f.Get(ctx, nil)
			inFlight--
		})
		futures = append(futures, f)
	}

	for _, f := range futures {
		_ = f.Get(ctx, nil)
	}
	return len(userIDs), nil
}
