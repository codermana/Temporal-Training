package reliability

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

// Heartbeat on every page. The heartbeat both reports progress (page becomes
// the resume point on retry, readable via activity.GetInfo) and is how a cancel
// is delivered: when the Workflow cancels, ctx.Done() fires here.
func ExportLargeTable(ctx context.Context, tableName string) (string, error) {
	for page := 0; page < 1000; page++ {
		select {
		case <-ctx.Done():
			cleanupPartialExport(tableName, page)
			return "", ctx.Err()
		default:
		}
		if err := exportPage(ctx, tableName, page); err != nil {
			return "", err
		}
		activity.RecordHeartbeat(ctx, page)
	}
	return fmt.Sprintf("s3://exports/%s", tableName), nil
}
