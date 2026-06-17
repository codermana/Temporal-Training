// The final "notify" step of an import — in AWS an SNS publish wrapped in a
// Lambda (or an EventBridge fan-out) — becomes an Activity that publishes to an
// SNS topic. Publishing is I/O, so it lives in an Activity, never in Workflow
// code. Subscribers (email, SQS, Lambda) stay decoupled: the publisher only
// knows the topic ARN, not who's listening.
//
// SNS delivery is at-least-once, so a retried Activity may publish the same
// notification twice. Make the message self-identifying — include the
// workflowId + runId — so consumers can dedup, and on a FIFO topic pass a
// message-deduplication id (the workflowId) so SNS itself collapses duplicates.
//
// The Go port of sns_publish_activity.java. The SNS call sits behind a small
// SnsAPI interface so the Temporal usage compiles without the AWS SDK; wire the
// real aws-sdk-go-v2 sns.Client (endpoint http://127.0.0.1:4566, us-east-1,
// test/test creds) to that interface for LocalStack.
package aws

import (
	"context"
	"encoding/json"

	"go.temporal.io/sdk/activity"
)

// SnsAPI is the slice of the AWS SNS client this Activity needs. dedupID is the
// FIFO message-deduplication id (the workflowId); ignored on a standard topic.
type SnsAPI interface {
	Publish(ctx context.Context, topicARN, subject, message, dedupID string) (messageID string, err error)
}

// SnsActivities wraps an SnsAPI so the Activity stays testable.
type SnsActivities struct{ Sns SnsAPI }

type importNotice struct {
	WorkflowID  string `json:"workflowId"`
	RunID       string `json:"runId"`
	RowCount    int64  `json:"rowCount"`
	OutputS3URI string `json:"outputS3Uri"`
}

// PublishNotification publishes the import result to an SNS topic and returns
// the SNS message id (surfaced in the UI as the Activity result).
func (a *SnsActivities) PublishNotification(ctx context.Context, topicARN, workflowID string, rowCount int64, outputS3URI string) (string, error) {
	// The workflowId + runId travel in the body so subscribers can dedup an
	// at-least-once redelivery; this is a small notification, not a data bus —
	// pass the output URI, never the rows themselves.
	body, err := json.Marshal(importNotice{
		WorkflowID:  workflowID,
		RunID:       activity.GetInfo(ctx).WorkflowExecution.RunID,
		RowCount:    rowCount,
		OutputS3URI: outputS3URI,
	})
	if err != nil {
		return "", err
	}

	// dedupID = workflowID: on a FIFO topic SNS collapses a retried publish.
	return a.Sns.Publish(ctx, topicARN, "import-complete", string(body), workflowID)
}
