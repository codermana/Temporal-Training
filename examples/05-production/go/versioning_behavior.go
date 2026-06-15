package production

import (
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Worker Versioning behavior. Go has no class annotation like Java's
// @WorkflowVersioningBehavior; the behavior is set per workflow type at
// registration via RegisterWorkflowOptions.VersioningBehavior. Pinned keeps
// existing executions on a compatible Build ID; AutoUpgrade lets long-runners
// move to newer compatible worker code.
func RegisterVersionedWorkflows(w worker.Worker) {
	w.RegisterWorkflowWithOptions(ShortLivedCheckoutWorkflow, workflow.RegisterOptions{
		Name:               "ShortLivedCheckoutWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})
	w.RegisterWorkflowWithOptions(SubscriptionLifecycleWorkflow, workflow.RegisterOptions{
		Name:               "SubscriptionLifecycleWorkflow",
		VersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
	})
}

// ShortLivedCheckoutWorkflow: existing executions stay on compatible workers.
func ShortLivedCheckoutWorkflow(ctx workflow.Context, cartID string) error {
	return nil
}

// SubscriptionLifecycleWorkflow: long-running executions can auto-upgrade.
func SubscriptionLifecycleWorkflow(ctx workflow.Context, subscriptionID string) error {
	return nil
}
