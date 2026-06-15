package saga

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

// Transfer is the money-transfer request.
type Transfer struct {
	FromAccount string
	ToAccount   string
	Amount      int
}

// MoneyTransferSaga is a ledger saga: debit the source account, then credit the
// destination. If the credit fails (frozen/closed account), refund the debit so
// the ledger never loses money. The debit returns a ledger entry id that the
// compensating refund needs — which is why forward steps return ids.
func MoneyTransferSaga(ctx workflow.Context, t Transfer) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var compensations []func()
	compensate := func() {
		for i := len(compensations) - 1; i >= 0; i-- {
			compensations[i]()
		}
	}

	var debitEntry string
	if err := workflow.ExecuteActivity(ctx, "Debit", t).Get(ctx, &debitEntry); err != nil {
		return "REVERSED", err
	}
	compensations = append(compensations, func() {
		_ = workflow.ExecuteActivity(ctx, "Refund", debitEntry).Get(ctx, nil)
	})

	if err := workflow.ExecuteActivity(ctx, "Credit", t).Get(ctx, nil); err != nil {
		compensate()
		return "REVERSED", nil
	}

	return "SETTLED", nil
}
