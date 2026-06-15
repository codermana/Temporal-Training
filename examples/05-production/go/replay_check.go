package production

import (
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// ReplayProductionHistory is the Go analogue of Java's
// WorkflowReplayer.replayWorkflowExecutionFromResource: register the current
// workflow code with a WorkflowReplayer, then feed it a history exported from a
// real run. ReplayWorkflowHistoryFromJSONFile returns a non-determinism error if
// today's code produces a divergent command stream.
func ReplayProductionHistory() error {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(OrderSagaWorkflow)
	return replayer.ReplayWorkflowHistoryFromJSONFile(nil, "histories/order-1001.json")
}

// OrderSagaWorkflow stands in for the workflow under test.
func OrderSagaWorkflow(ctx workflow.Context) error { return nil }
