package interactions

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// A Schedule is a server-managed cron replacement. The interval spec fires every
// fixed period; jitter spreads the fire time to avoid thundering herds, and the
// overlap policy decides what happens when a run is still going as the next fires.
func CreateHourlySchedule(ctx context.Context, c client.Client) error {
	_, err := c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: "hourly-orders",
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{{Every: time.Hour}},
			Jitter:    5 * time.Minute,
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        "hourly-orders-wf",
			Workflow:  OrdersWorkflow,
			Args:      []any{"hourly"},
			TaskQueue: "orders",
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
	})
	return err
}
