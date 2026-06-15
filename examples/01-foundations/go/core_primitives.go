package foundations

import (
	"context"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// DAG -> Workflow: durable orchestration state and decisions.
func LoanWorkflow(ctx workflow.Context, applicationID string) (string, error) {
	return "", nil
}

// Operator -> Activity: unreliable work with retries, timeouts, side effects.
// In Go, Activities are plain functions whose first arg is context.Context.
func PullCreditScore(ctx context.Context, applicationID string) (int, error) {
	return 0, nil
}

func NotifyApplicant(ctx context.Context, applicationID, decision string) error {
	return nil
}

// Executor -> Worker: a long-running process polling a Task Queue.
func start(c client.Client) error {
	w := worker.New(c, "loan-decisions", worker.Options{})
	w.RegisterWorkflow(LoanWorkflow)
	w.RegisterActivity(PullCreditScore)
	w.RegisterActivity(NotifyApplicant)
	return w.Run(worker.InterruptCh())
}
