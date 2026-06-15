package production

import (
	"context"

	"go.temporal.io/sdk/activity"
)

// Custom Activity metric. activity.GetMetricsHandler(ctx) returns the same
// handler the SDK uses, wired to whatever client.MetricsHandler was configured
// (see prometheus_metrics.go), so a custom counter is exported alongside the
// built-in SDK metrics. The Go analogue of holding a Micrometer Counter in
// InvoiceActivitiesImpl.
func GenerateInvoice(ctx context.Context, orderID string) (string, error) {
	activity.GetMetricsHandler(ctx).
		Counter("training_invoices_generated_total").
		Inc(1)
	return "s3://invoices/" + orderID + ".pdf", nil
}
