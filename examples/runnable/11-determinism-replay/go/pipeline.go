// Package main holds the determinism-replay lab: a data pipeline Workflow plus a
// "harmless looking" reordered refactor. The Go equivalent of the Java
// determinism-replay lab. The original extracts THEN loads; the reordered version
// loads THEN extracts. Both register under the SAME Workflow type name, so a
// history recorded from the original replays against the reordered code — and the
// different command order trips a non-determinism error. That regression is
// exactly what replay testing catches.
package main

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	TaskQueue    = "replay-demo"
	WorkflowName = "DataPipelineWorkflow"
)

// Both impls register under the SAME Workflow type name, so a history recorded
// from one is replayed against the other.
var registerOpts = workflow.RegisterOptions{Name: WorkflowName}

// Extract is an Activity returning the extracted rows.
func Extract(ctx context.Context) (string, error) {
	return "rows:100", nil
}

// Load is an Activity that loads the given data.
func Load(ctx context.Context, data string) (string, error) {
	return "loaded " + data, nil
}

// DataPipelineWorkflow is the original, shipped version: extract THEN load.
func DataPipelineWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var extracted string
	if err := workflow.ExecuteActivity(ctx, Extract).Get(ctx, &extracted); err != nil {
		return "", err
	}
	var loaded string
	if err := workflow.ExecuteActivity(ctx, Load, extracted).Get(ctx, &loaded); err != nil {
		return "", err
	}
	return loaded, nil
}

// ReorderedPipelineWorkflow is the breaking refactor: load THEN extract. Same
// type name, new command order.
func ReorderedPipelineWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var loaded string
	if err := workflow.ExecuteActivity(ctx, Load, "rows:0").Get(ctx, &loaded); err != nil {
		return "", err
	}
	var extracted string
	if err := workflow.ExecuteActivity(ctx, Extract).Get(ctx, &extracted); err != nil {
		return "", err
	}
	return loaded + " / " + extracted, nil
}
