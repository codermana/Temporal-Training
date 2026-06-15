// Package main holds the schedules lab: create a daily Schedule that fires a
// trivial DailyReportWorkflow at 09:00. The Go equivalent of CreateSchedule.java.
package main

import (
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "reports"

// DailyReportWorkflow is the Workflow a Schedule fires. Kept trivial on purpose:
// the lab is about the Schedule, not the report.
func DailyReportWorkflow(ctx workflow.Context, reportName string) error {
	workflow.GetLogger(ctx).Info("building report", "name", reportName)
	return nil
}
