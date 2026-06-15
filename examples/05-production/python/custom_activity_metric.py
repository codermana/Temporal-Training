from temporalio import activity

# Custom Activity metric. The SDK gives every Activity a MetricMeter wired to the
# same Prometheus runtime (see prometheus_metrics.py), so a custom counter shows
# up alongside the built-in SDK metrics. This is the Python analogue of holding a
# Micrometer Counter in InvoiceActivitiesImpl.


@activity.defn
async def generate_invoice(order_id: str) -> str:
    counter = activity.metric_meter().create_counter(
        "training_invoices_generated_total",
        "Number of invoices generated",
    )
    counter.add(1)
    return f"s3://invoices/{order_id}.pdf"
