// Package wordcount holds the word-count fan-out lab: count chunks in parallel,
// then sum the partial totals.
package wordcount

import (
	"context"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const TaskQueue = "word-count"

var SampleChunks = []string{
	"Temporal workflows keep the overall document job durable.",
	"Each activity receives one chunk and counts its words.",
	"The workflow starts every chunk count before waiting for results.",
	"Fan-in sums the partial counts into one final total.",
}

// CountWords is an Activity: heartbeat, then count words in one chunk.
func CountWords(ctx context.Context, chunk string) (int, error) {
	activity.RecordHeartbeat(ctx, "counting chunk")
	return len(strings.Fields(chunk)), nil
}

// WordCountWorkflow counts each chunk in parallel and returns the total.
func WordCountWorkflow(ctx workflow.Context, chunks []string) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Second,
			MaximumAttempts: 3,
		},
	})

	futures := make([]workflow.Future, 0, len(chunks))
	for _, chunk := range chunks {
		futures = append(futures, workflow.ExecuteActivity(ctx, CountWords, chunk))
	}

	total := 0
	for _, f := range futures {
		var count int
		if err := f.Get(ctx, &count); err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}
