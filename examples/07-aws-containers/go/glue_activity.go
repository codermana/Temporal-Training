// Run a long Glue ETL job from inside an Activity: submit, heartbeat the run id
// while polling, and return a structured ApplicationError on a bad terminal state.
//
// The Go port of glue_activity.java. The AWS Glue calls sit behind a small
// GlueAPI interface so the Temporal usage compiles without the AWS SDK; wire the
// real aws-sdk-go-v2 glue.Client to that interface in production.
package aws

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// GlueAPI is the slice of the AWS Glue client this Activity needs.
type GlueAPI interface {
	StartJobRun(ctx context.Context, jobName, inputS3URI string) (runID string, err error)
	JobRunState(ctx context.Context, jobName, runID string) (state string, errMsg string, err error)
}

// GlueActivities wraps a GlueAPI so the Activity stays testable.
type GlueActivities struct{ Glue GlueAPI }

// RunGlueJob submits a Glue run, heartbeats the run id while polling, and maps
// terminal states to success vs. a typed failure.
func (a *GlueActivities) RunGlueJob(ctx context.Context, jobName, inputS3URI string) (string, error) {
	runID, err := a.Glue.StartJobRun(ctx, jobName, inputS3URI)
	if err != nil {
		return "", err
	}

	for {
		activity.RecordHeartbeat(ctx, runID)
		state, errMsg, err := a.Glue.JobRunState(ctx, jobName, runID)
		if err != nil {
			return "", err
		}

		switch state {
		case "SUCCEEDED":
			return runID, nil
		case "FAILED", "TIMEOUT", "STOPPED":
			// A typed failure surfaces in the UI as GlueJobFailed, not a stack trace.
			return "", temporal.NewApplicationError(
				fmt.Sprintf("Glue job %s ended as %s: %s", jobName, state, errMsg),
				"GlueJobFailed",
			)
		}

		// Back off between polls so we don't hammer the Glue API and burn the
		// throttle limit before the job finishes.
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(15 * time.Second):
		}
	}
}
