package foundations

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// The same shape as the Airflow DAG in ../airflow_dag_before.py, expressed as
// durable application code: the dependency graph is just sequential Get calls,
// and the data passed between steps is ordinary return values — no XComs.
func OrdersWorkflow(ctx workflow.Context, batchDate string) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
	})

	var rawURI, cleanURI string
	if err := workflow.ExecuteActivity(ctx, "Extract", batchDate).Get(ctx, &rawURI); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, "Transform", rawURI).Get(ctx, &cleanURI); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, "Load", cleanURI).Get(ctx, nil)
}
