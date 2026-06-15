package production

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// The Go analogue of the JUnit 5 + Mockito Activity-mocking test. testsuite's
// TestWorkflowEnvironment runs the workflow in-process and skips time, and
// env.OnActivity(...).Return(...) is the testify-mock stand-in for the real
// Activity — no I/O in tests.

func ReminderWorkflow(ctx workflow.Context, userID string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	var email string
	if err := workflow.ExecuteActivity(ctx, LookupEmail, userID).Get(ctx, &email); err != nil {
		return "", err
	}
	return "sent to " + email, nil
}

func LookupEmail(ctx context.Context, userID string) (string, error) {
	return "", nil // real impl does I/O; tests mock it
}

func TestCompletesWithMockedActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	// mock.Anything matches the activity's context.Context argument.
	env.OnActivity(LookupEmail, mock.Anything, "u1").Return("u1@example.com", nil)

	env.ExecuteWorkflow(ReminderWorkflow, "u1")

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result string
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "sent to u1@example.com", result)
	env.AssertExpectations(t)
}
