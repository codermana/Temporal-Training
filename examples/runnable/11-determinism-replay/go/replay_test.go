package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// recordHistory runs the shipped workflow once against an in-process dev server
// (downloaded + started by the SDK, no external server) and returns its event
// history — the Go analogue of the Java test's recordHistory().
func recordHistory(t *testing.T) *historypb.History {
	t.Helper()
	ctx := context.Background()

	server, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{})
	require.NoError(t, err)
	defer func() { _ = server.Stop() }()

	c := server.Client()

	w := worker.New(c, TaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(DataPipelineWorkflow, registerOpts)
	w.RegisterActivity(Extract)
	w.RegisterActivity(Load)
	require.NoError(t, w.Start())
	defer w.Stop()

	run, err := c.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{ID: "pipeline-1", TaskQueue: TaskQueue},
		WorkflowName)
	require.NoError(t, err)
	require.NoError(t, run.Get(ctx, nil))

	iter := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	hist := &historypb.History{}
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		hist.Events = append(hist.Events, event)
	}
	return hist
}

// TestShippedCodeReplaysClean: the recorded history still matches current code.
func TestShippedCodeReplaysClean(t *testing.T) {
	history := recordHistory(t)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(DataPipelineWorkflow, registerOpts)
	// No error == replay-compatible.
	require.NoError(t, replayer.ReplayWorkflowHistory(nil, history))
}

// TestReorderedCodeBreaksReplay: the reordered refactor diverges -> replay fails.
func TestReorderedCodeBreaksReplay(t *testing.T) {
	history := recordHistory(t)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(ReorderedPipelineWorkflow, registerOpts)
	// Different commands than the recorded history -> non-determinism error.
	require.Error(t, replayer.ReplayWorkflowHistory(nil, history))
}
