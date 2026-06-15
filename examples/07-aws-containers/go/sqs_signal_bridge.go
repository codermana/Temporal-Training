// An "EventBridge rule -> Lambda -> StartExecution" trigger becomes a long-poll
// SQS consumer that starts or signals Workflows. The bridge is plain glue code
// (it runs outside any Workflow): receive a message, translate it to a
// SignalWithStartWorkflow, delete it. SignalWithStart is idempotent on the
// Workflow ID, so an at-least-once SQS redelivery just re-signals the same run.
//
// The SQS calls sit behind a small SqsAPI interface so the Temporal usage
// compiles without the AWS SDK.
package aws

import (
	"context"
	"encoding/json"

	"go.temporal.io/sdk/client"
)

// SqsMessage is one received message; ReceiptHandle is needed to delete it.
type SqsMessage struct {
	Body          string
	ReceiptHandle string
}

// SqsAPI is the slice of the AWS SQS client this bridge needs.
type SqsAPI interface {
	Receive(ctx context.Context, queueURL string) ([]SqsMessage, error)
	Delete(ctx context.Context, queueURL, receiptHandle string) error
}

type fileEvent struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	S3URI  string `json:"s3Uri"`
}

// Pump long-polls SQS and signal-with-starts a Workflow per message.
func Pump(ctx context.Context, c client.Client, sqs SqsAPI, queueURL string) error {
	for {
		msgs, err := sqs.Receive(ctx, queueURL) // long poll inside the impl
		if err != nil {
			return err
		}
		for _, m := range msgs {
			var ev fileEvent
			if err := json.Unmarshal([]byte(m.Body), &ev); err != nil {
				return err
			}

			// SignalWithStart: starts the Workflow if absent, signals it if running.
			_, err := c.SignalWithStartWorkflow(
				ctx,
				"import-"+ev.Bucket+"-"+ev.Key,
				"file_arrived", ev.S3URI,
				client.StartWorkflowOptions{TaskQueue: "transform"},
				"ImportWorkflow", ev.S3URI,
			)
			if err != nil {
				return err
			}

			// Delete only after the signal is durable in Temporal — at-least-once.
			if err := sqs.Delete(ctx, queueURL, m.ReceiptHandle); err != nil {
				return err
			}
		}
	}
}
