// Package main holds the import pipeline (validate -> transform -> load) as a
// Workflow + Activities. The Go equivalent of the Java ImportWorkflow lab.
// Activities pass S3 *URIs* forward, never file bytes, so Workflow history stays
// small. The bodies fake the work; in the lab they'd call LocalStack S3.
package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "transform"

// Validate rewrites the incoming URI to a /validated/ key.
func Validate(ctx context.Context, inputS3URI string) (string, error) {
	time.Sleep(250 * time.Millisecond)
	activity.RecordHeartbeat(ctx, "validated")
	return strings.Replace(inputS3URI, "/incoming/", "/validated/", 1), nil
}

// Transform rewrites the validated URI to a /transformed/ key.
func Transform(ctx context.Context, validatedS3URI string) (string, error) {
	time.Sleep(time.Second)
	activity.RecordHeartbeat(ctx, "transformed")
	return strings.Replace(validatedS3URI, "/validated/", "/transformed/", 1), nil
}

// Load returns a synthetic row count for the transformed object.
func Load(ctx context.Context, transformedS3URI string) (int64, error) {
	time.Sleep(500 * time.Millisecond)
	h := fnv.New32a()
	_, _ = h.Write([]byte(transformedS3URI))
	return int64(h.Sum32() % 10_000), nil
}

// ImportWorkflow runs validate -> transform -> load and returns the final URI.
func ImportWorkflow(ctx workflow.Context, inputS3URI string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
	})

	var validatedURI string
	if err := workflow.ExecuteActivity(ctx, Validate, inputS3URI).Get(ctx, &validatedURI); err != nil {
		return "", err
	}
	var transformedURI string
	if err := workflow.ExecuteActivity(ctx, Transform, validatedURI).Get(ctx, &transformedURI); err != nil {
		return "", err
	}
	var rowCount int64
	if err := workflow.ExecuteActivity(ctx, Load, transformedURI).Get(ctx, &rowCount); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s?rows=%d", transformedURI, rowCount), nil
}
