package interactions

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// Airflow schedule_interval + catchup map directly onto a Temporal Schedule.
func CreateDailyCron(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: "daily-orders-cron",
		Spec: client.ScheduleSpec{
			// schedule_interval -> cron strings, intervals, OR calendars.
			CronExpressions: []string{"0 9 * * *"}, // 09:00 every day
			Jitter:          5 * time.Minute,
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        "daily-orders-wf",
			Workflow:  OrdersWorkflow,
			Args:      []any{"daily"},
			TaskQueue: "orders",
		},
		// catchup -> bounded window in which missed runs are fired.
		CatchupWindow: time.Hour,
		Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
	return err
}

// Overlap policy = what happens when a run is still going when the next fires:
//   SKIP            - drop the new run (Airflow max_active_runs=1, catchup off)
//   BUFFER_ONE      - queue exactly one to run next
//   BUFFER_ALL      - queue every missed run
//   ALLOW_ALL       - run them concurrently
//   CANCEL_OTHER    - cancel the running one, then start
//   TERMINATE_OTHER - terminate the running one, then start
