package saga

import (
	"go.temporal.io/sdk/workflow"
)

// SubscriptionWorkflow is an indefinitely-running, event-driven workflow. After a
// bounded number of events it returns a ContinueAsNew error to start a fresh run
// with a clean history, carrying forward only the state the next run needs.
//
// hasNextEvent / handleNextEvent stand in for signal-driven state (a real app
// would read a signal channel here).
func SubscriptionWorkflow(ctx workflow.Context, subscriptionID string, eventCount int) error {
	for {
		_ = workflow.Await(ctx, func() bool { return hasNextEvent(ctx) })
		handleNextEvent(ctx)
		eventCount++

		if eventCount >= 1000 {
			// Replaces this run with a new one (eventCount reset to 0).
			return workflow.NewContinueAsNewError(ctx, SubscriptionWorkflow, subscriptionID, 0)
		}
	}
}
