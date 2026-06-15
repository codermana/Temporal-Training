package reliability

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// Real-world: a multi-hour data backfill that must resume where it left off
// after a worker crash or activity retry — not restart from page 0. The
// heartbeat carries the last completed page; on a retry the next attempt reads
// it back (activity.GetHeartbeatDetails) and skips the work already done.
func Backfill(ctx context.Context, dataset string) (string, error) {
	startPage := 0
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &startPage) // resume point
	}

	for page := startPage; page < 100_000; page++ {
		copyPage(dataset, page)
		activity.RecordHeartbeat(ctx, page) // checkpoint: this page is done
	}
	return "backfilled " + dataset, nil
}
