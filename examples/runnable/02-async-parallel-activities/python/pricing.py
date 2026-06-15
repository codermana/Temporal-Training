"""Order pricing workflow: price every SKU in parallel, then sum.

The Python equivalent of the Java OrderPricingWorkflow lab. Activities run
concurrently because we start them all before awaiting any of them.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

TASK_QUEUE = "pricing"

PRICES = {"book": 30, "lamp": 75, "desk": 250}


class PricingActivities:
    @activity.defn
    async def price(self, sku: str) -> int:
        activity.heartbeat(f"pricing {sku}")
        return PRICES.get(sku, 10)


@workflow.defn
class OrderPricingWorkflow:
    @workflow.run
    async def total(self, skus: list[str]) -> int:
        # Start one Activity per SKU before awaiting any → they run in parallel.
        prices = [
            workflow.execute_activity_method(
                PricingActivities.price,
                sku,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1), maximum_attempts=3
                ),
            )
            for sku in skus
        ]
        return sum(await asyncio.gather(*prices))
