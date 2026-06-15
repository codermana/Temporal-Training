// Package retries holds the retries + heartbeat lab. The Go equivalent of the
// Java retries lab. ChargeCard fails its first two attempts so the RetryPolicy is
// visible in history; ExportLargeReport heartbeats once per page so a Worker
// restart resumes mid-flight from the last recorded page instead of starting over.
//
// The Workflow and Activity definitions live here so both standalone commands
// can import them: ./worker (registers + polls) and ./starter (starts a run).
package retries

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "retries-heartbeats"

// FlakyActivities holds state shared across attempts on this Worker, so the
// charge counter survives retries (mirrors the Java AtomicInteger).
type FlakyActivities struct {
	chargeAttempts int32
}

// ChargeCard fails the first two attempts, then succeeds.
func (a *FlakyActivities) ChargeCard(ctx context.Context, orderID string) (string, error) {
	attempt := atomic.AddInt32(&a.chargeAttempts, 1)
	log.Printf("ChargeCard attempt %d for %s", attempt, orderID)
	if attempt < 3 {
		// A returned error is retried per the Workflow's RetryPolicy.
		return "", temporal.NewApplicationError(
			fmt.Sprintf("payment gateway timeout (attempt %d)", attempt), "GatewayTimeout")
	}
	return fmt.Sprintf("charged %s on attempt %d", orderID, attempt), nil
}

// ExportLargeReport heartbeats once per page so a restart resumes mid-flight.
func (a *FlakyActivities) ExportLargeReport(ctx context.Context, pages int) (string, error) {
	// On retry, resume from the last recorded heartbeat instead of page 0.
	startPage := 0
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &startPage)
	}
	for page := startPage; page < pages; page++ {
		time.Sleep(300 * time.Millisecond)
		log.Printf("exported page %d/%d", page+1, pages)
		activity.RecordHeartbeat(ctx, page+1)
	}
	return fmt.Sprintf("exported %d pages", pages), nil
}

// ProcessingWorkflow charges the card (with retries) then exports the report
// (with a heartbeat timeout).
func ProcessingWorkflow(ctx workflow.Context, orderID string) (string, error) {
	// A deliberate retry policy: 5 attempts, 1s initial backoff doubling each time.
	chargeCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    5,
		},
	})

	var a *FlakyActivities
	var charge string
	if err := workflow.ExecuteActivity(chargeCtx, a.ChargeCard, orderID).Get(ctx, &charge); err != nil {
		return "", err
	}

	// A long Activity: the heartbeat timeout detects a dead Worker between pages.
	reportCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    5 * time.Second,
	})
	var exported string
	if err := workflow.ExecuteActivity(reportCtx, a.ExportLargeReport, 5).Get(ctx, &exported); err != nil {
		return "", err
	}

	return charge + " | " + exported, nil
}
