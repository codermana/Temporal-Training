// Package hello holds the Hello Temporal lab: a Workflow that calls one
// Activity and returns its greeting. The Go equivalent of the Java
// GreetingWorkflow lab.
//
// The Workflow and Activity definitions live here so both standalone commands
// can import them: ./worker (registers + polls) and ./starter (starts a run).
package hello

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "hello-temporal"

// ComposeGreeting is an Activity (first arg is context.Context).
func ComposeGreeting(ctx context.Context, name string) (string, error) {
	return "Hello, " + name + " from a Temporal Activity", nil
}

// GreetingWorkflow calls the Activity and returns its result.
func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var greeting string
	err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &greeting)
	return greeting, err
}
