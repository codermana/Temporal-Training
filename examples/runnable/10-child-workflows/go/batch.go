// Package main holds the parent/child lab. The Go equivalent of the Java
// child-workflows lab. The parent starts one child per item - each with its own
// stable Workflow ID, so it is separately queryable / signalable / cancelable -
// then waits for all of them.
package main

import (
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "child-workflows"

// ItemWorkflow is a child Workflow - its own Workflow ID, its own history,
// separately addressable.
func ItemWorkflow(ctx workflow.Context, item string) (string, error) {
	// A durable sleep so each child is visibly "running" in the Web UI for a moment.
	if err := workflow.Sleep(ctx, 500*time.Millisecond); err != nil {
		return "", err
	}
	return "processed[" + item + "] in child " + workflow.GetInfo(ctx).WorkflowExecution.ID, nil
}

// BatchWorkflow starts one child per item in parallel, then collects the results.
func BatchWorkflow(ctx workflow.Context, items []string) (string, error) {
	futures := make([]workflow.ChildWorkflowFuture, 0, len(items))
	for _, item := range items {
		// Each child gets a stable, independent Workflow ID.
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: "item-" + item,
		})
		futures = append(futures, workflow.ExecuteChildWorkflow(childCtx, ItemWorkflow, item))
	}

	results := make([]string, 0, len(items))
	for _, f := range futures {
		var result string
		if err := f.Get(ctx, &result); err != nil {
			return "", err
		}
		results = append(results, result)
	}
	return strings.Join(results, "\n"), nil
}
