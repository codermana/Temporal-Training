package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

// TestSkipsWorkflowTime proves the one-day sleep is skipped: the test completes
// in milliseconds. testsuite.TestWorkflowEnvironment runs in-process with no
// Temporal server and auto-skips timers.
func TestSkipsWorkflowTime(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(ReminderWorkflow, "ship report")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "Reminder: ship report", result)
}

// TestMocksTheActivity is the testify-mock analogue of the Mockito test: stub the
// Activity with OnActivity(...).Return(...) so no real I/O runs.
func TestMocksTheActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.OnActivity(LookupEmail, mock.Anything, "u1").Return("u1@example.com", nil)

	env.ExecuteWorkflow(EmailReminderWorkflow, "u1")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "sent to u1@example.com", result)
	env.AssertExpectations(t)
}

// TestActivityFailureSurfaces is the negative path: a mocked Activity that always
// errors makes the Workflow fail.
func TestActivityFailureSurfaces(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.OnActivity(LookupEmail, mock.Anything, "u1").
		Return("", errors.New("user service down"))

	env.ExecuteWorkflow(EmailReminderWorkflow, "u1")

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())
}
