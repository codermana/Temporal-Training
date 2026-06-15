package reliability

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Fan a void Activity out across many ids. Collect the Futures first, then Get
// each one — the equivalent of starting them all and Promise.allOf-ing.
func NotifyEveryone(ctx workflow.Context, userIDs []string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	futures := make([]workflow.Future, 0, len(userIDs))
	for _, id := range userIDs {
		futures = append(futures, workflow.ExecuteActivity(ctx, "Send", id))
	}
	for _, f := range futures {
		if err := f.Get(ctx, nil); err != nil {
			return err
		}
	}
	return nil
}

// A Selector is the equivalent of Promise.anyOf: continue as soon as the FIRST
// branch finishes (primary vs fallback provider).
func FirstToAnswer(ctx workflow.Context, query string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	})

	primary := workflow.ExecuteActivity(ctx, "AskPrimary", query)
	fallback := workflow.ExecuteActivity(ctx, "AskFallback", query)

	var answer string
	workflow.NewSelector(ctx).
		AddFuture(primary, func(f workflow.Future) { _ = f.Get(ctx, &answer) }).
		AddFuture(fallback, func(f workflow.Future) { _ = f.Get(ctx, &answer) }).
		Select(ctx)
	return answer, nil
}
