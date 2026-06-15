package interactions

import (
	"errors"

	"go.temporal.io/sdk/workflow"
)

// An Update handler is registered with workflow.SetUpdateHandlerWithOptions. The
// Validator runs first on the accepted path: returning an error rejects the
// Update before it mutates state (the Go equivalent of @UpdateValidatorMethod).
func CartWorkflow(ctx workflow.Context, cartID string) (string, error) {
	items := map[string]int{}

	err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		"addItem",
		func(ctx workflow.Context, sku string, quantity int) (int, error) {
			items[sku] += quantity
			total := 0
			for _, n := range items {
				total += n
			}
			return total, nil
		},
		workflow.UpdateHandlerOptions{
			Validator: func(ctx workflow.Context, sku string, quantity int) error {
				if quantity <= 0 {
					return errors.New("quantity must be positive")
				}
				return nil
			},
		},
	)
	if err != nil {
		return "", err
	}

	// Wait until the cart is checked out (driven by a real signal in practice).
	_ = workflow.Await(ctx, func() bool { return false })
	return cartID, nil
}
