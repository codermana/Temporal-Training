package production

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// NEW production snippet: emit typed Search Attributes from inside a Workflow so
// ops dashboards / `temporal workflow list` can filter and group live executions
// (e.g. "all orders in SHIPPED for tenant acme"). Register the custom keys once
// per namespace first:
//
//	temporal operator search-attribute create --name OrderStage --type Keyword
//	temporal operator search-attribute create --name Tenant     --type Keyword
var (
	orderStage = temporal.NewSearchAttributeKeyKeyword("OrderStage")
	tenant     = temporal.NewSearchAttributeKeyKeyword("Tenant")
)

// OrderWorkflow tags itself with ops Search Attributes as it advances.
func OrderWorkflow(ctx workflow.Context, orderID, tenantID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	// Set at start so the order shows up immediately in ops queries.
	if err := workflow.UpsertTypedSearchAttributes(ctx,
		tenant.ValueSet(tenantID), orderStage.ValueSet("RECEIVED")); err != nil {
		return "", err
	}

	if err := workflow.ExecuteActivity(ctx, "Charge", orderID).Get(ctx, nil); err != nil {
		return "", err
	}
	// Update the stage as the order moves through the pipeline.
	if err := workflow.UpsertTypedSearchAttributes(ctx, orderStage.ValueSet("CHARGED")); err != nil {
		return "", err
	}

	if err := workflow.ExecuteActivity(ctx, "Ship", orderID).Get(ctx, nil); err != nil {
		return "", err
	}
	if err := workflow.UpsertTypedSearchAttributes(ctx, orderStage.ValueSet("SHIPPED")); err != nil {
		return "", err
	}
	return "done", nil
}
