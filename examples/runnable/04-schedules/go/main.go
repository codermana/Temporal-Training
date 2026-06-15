// Command schedules creates a daily Schedule that fires DailyReportWorkflow at
// 09:00, mirroring CreateSchedule.java.
//
//	go run .        // needs a Temporal dev server on 127.0.0.1:7233
//
// This is a one-shot client program, not a long-lived Worker. It registers the
// Schedule on the server and exits; the server fires the Workflow on the spec
// from then on. Run a Worker on the 'reports' task queue separately if you want
// the scheduled runs to actually execute.
package main

import (
	"context"
	"log"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const scheduleID = "daily-sales-report-schedule"

func main() {
	c, err := client.Dial(client.Options{HostPort: "127.0.0.1:7233"})
	if err != nil {
		log.Fatalln("unable to create client:", err)
	}
	defer c.Close()

	_, err = c.ScheduleClient().Create(context.Background(), client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			// 09:00 every day - the calendar equivalent of the Java
			// ScheduleCalendarSpec(hour=9, minute=0).
			Calendars: []client.ScheduleCalendarSpec{{
				Hour:   []client.ScheduleRange{{Start: 9}},
				Minute: []client.ScheduleRange{{Start: 0}},
			}},
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        "daily-sales-report",
			Workflow:  DailyReportWorkflow,
			Args:      []any{"daily-sales"},
			TaskQueue: TaskQueue,
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		Note:    "Airflow daily DAG replacement",
	})
	if err != nil {
		log.Fatalln("unable to create schedule:", err)
	}

	log.Printf("Created Schedule '%s' (daily at 09:00, overlap=SKIP).", scheduleID)
	log.Println("Inspect it with:  temporal schedule describe --schedule-id " + scheduleID)
}
