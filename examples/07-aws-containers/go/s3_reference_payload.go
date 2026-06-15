// Keep Workflow history small: pass S3 references (URIs), not file contents,
// between steps. The Activities do the heavy I/O; the Workflow only sequences URIs.
//
// The Go port of s3_reference_payload.java. The AWS S3 calls sit behind an S3API
// interface so the Temporal usage compiles without the AWS SDK.
package aws

import (
	"context"
	"time"

	"go.temporal.io/sdk/workflow"
)

// TransformRequest and TransformResult carry S3 URIs, never bytes.
type TransformRequest struct {
	InputS3URI   string
	OutputPrefix string
}

type TransformResult struct {
	OutputS3URI string
	RowCount    int64
}

// S3API is the slice of the AWS S3 client these Activities need.
type S3API interface {
	Copy(ctx context.Context, srcURI, dstURI string) error
	RowCount(ctx context.Context, uri string) (int64, error)
}

// TransformActivities wraps an S3API so the Activities stay testable.
type TransformActivities struct{ S3 S3API }

// Transform reads from input, writes under outputPrefix, and returns the *new*
// URI - never the bytes.
func (a *TransformActivities) Transform(ctx context.Context, inputS3URI, outputPrefix string) (string, error) {
	out := outputPrefix + "/transformed.parquet"
	if err := a.S3.Copy(ctx, inputS3URI, out); err != nil {
		return "", err
	}
	return out, nil
}

// CountRows returns the row count of an output object.
func (a *TransformActivities) CountRows(ctx context.Context, outputS3URI string) (int64, error) {
	return a.S3.RowCount(ctx, outputS3URI)
}

// TransformWorkflow sequences the two Activities, passing only S3 references so
// Workflow history stays lean.
func TransformWorkflow(ctx workflow.Context, req TransformRequest) (TransformResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
	})

	var a *TransformActivities
	var outputURI string
	if err := workflow.ExecuteActivity(ctx, a.Transform, req.InputS3URI, req.OutputPrefix).
		Get(ctx, &outputURI); err != nil {
		return TransformResult{}, err
	}

	var rowCount int64
	if err := workflow.ExecuteActivity(ctx, a.CountRows, outputURI).Get(ctx, &rowCount); err != nil {
		return TransformResult{}, err
	}
	return TransformResult{OutputS3URI: outputURI, RowCount: rowCount}, nil
}
