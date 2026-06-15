// Package parallel holds the order pricing lab: price every SKU in parallel,
// then sum. The Go equivalent of the Java OrderPricingWorkflow lab. Activities
// run concurrently because we collect every Future before calling Get on any.
//
// The Workflow and Activity definitions live here so both standalone commands
// can import them: ./worker (registers + polls) and ./starter (starts a run).
package parallel

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "pricing"

var prices = map[string]int{"book": 30, "lamp": 75, "desk": 250}

// Price is an Activity: heartbeat, then return the SKU's price.
func Price(ctx context.Context, sku string) (int, error) {
	activity.RecordHeartbeat(ctx, "pricing "+sku)
	if p, ok := prices[sku]; ok {
		return p, nil
	}
	return 10, nil
}

// OrderPricingWorkflow prices each SKU in parallel and returns the total.
func OrderPricingWorkflow(ctx workflow.Context, skus []string) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
	})

	// Start one Activity per SKU before Get-ing any → they run in parallel.
	futures := make([]workflow.Future, 0, len(skus))
	for _, sku := range skus {
		futures = append(futures, workflow.ExecuteActivity(ctx, Price, sku))
	}

	total := 0
	for _, f := range futures {
		var price int
		if err := f.Get(ctx, &price); err != nil {
			return 0, err
		}
		total += price
	}
	return total, nil
}
