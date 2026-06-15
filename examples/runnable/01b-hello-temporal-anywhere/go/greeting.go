// Env-driven Hello Temporal (Lab 1.2b Docker / 1.2c Cloud), Go version.
package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "hello-anywhere"

func ComposeGreeting(ctx context.Context, name string) (string, error) {
	return "Hello, " + name + " from a Temporal Activity", nil
}

func GreetingWorkflow(ctx workflow.Context, name string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var greeting string
	err := workflow.ExecuteActivity(ctx, ComposeGreeting, name).Get(ctx, &greeting)
	return greeting, err
}
