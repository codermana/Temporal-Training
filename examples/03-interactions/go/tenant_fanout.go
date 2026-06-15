package interactions

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Real-world fan-out: a nightly billing run spawns one child Workflow per tenant.
// Each tenant gets its own Workflow ID (independently queryable / retryable), and
// PARENT_CLOSE_POLICY_ABANDON lets long tenant jobs outlive the coordinator. The
// parent starts every child before getting any, so all tenants bill in parallel.
func TenantBillingFanout(ctx workflow.Context, tenantIDs []string) (map[string]string, error) {
	futures := make(map[string]workflow.ChildWorkflowFuture, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:               "billing-" + tenantID,
			TaskQueue:                "billing",
			ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_ABANDON,
			WorkflowExecutionTimeout: time.Hour,
			RetryPolicy:              &temporal.RetryPolicy{MaximumAttempts: 3},
		})
		futures[tenantID] = workflow.ExecuteChildWorkflow(childCtx, TenantBillingWorkflow, tenantID)
	}

	results := make(map[string]string, len(tenantIDs))
	for tenantID, f := range futures {
		var result string
		if err := f.Get(ctx, &result); err != nil {
			return nil, err
		}
		results[tenantID] = result
	}
	return results, nil
}
