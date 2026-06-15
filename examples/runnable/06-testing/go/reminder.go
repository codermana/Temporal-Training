// Package main holds the reminder testing lab: a Workflow that sleeps a day then
// returns a message, plus an Activity-backed variant. The Go equivalent of the
// Java ReminderWorkflow testing lab. The testsuite env (see reminder_test.go)
// runs these in-process and skips time, so the one-day sleep costs nothing.
package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "test-reminder"

// LookupEmail is an Activity. The real impl would hit a user service; the test
// mocks it.
func LookupEmail(ctx context.Context, userID string) (string, error) {
	return "", nil
}

// ReminderWorkflow sleeps a day (skipped by the test env), then returns.
func ReminderWorkflow(ctx workflow.Context, message string) (string, error) {
	if err := workflow.Sleep(ctx, 24*time.Hour); err != nil {
		return "", err
	}
	return "Reminder: " + message, nil
}

// EmailReminderWorkflow calls an Activity, so a test can mock it.
func EmailReminderWorkflow(ctx workflow.Context, userID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var email string
	if err := workflow.ExecuteActivity(ctx, LookupEmail, userID).Get(ctx, &email); err != nil {
		return "", err
	}
	return "sent to " + email, nil
}
