"""Retry + heartbeat deep-dive. The Python equivalent of the Java retries lab.

chargeCard fails its first two attempts so the RetryPolicy is visible in history;
exportLargeReport heartbeats once per page so a Worker restart resumes mid-flight
from the last recorded page instead of starting over.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

TASK_QUEUE = "retries-heartbeats"


class FlakyActivities:
    def __init__(self) -> None:
        # One instance is shared across attempts on this Worker, so the counter
        # survives retries (mirrors the Java AtomicInteger).
        self._charge_attempts = 0

    @activity.defn
    async def charge_card(self, order_id: str) -> str:
        self._charge_attempts += 1
        attempt = self._charge_attempts
        print(f"charge_card attempt {attempt} for {order_id}")
        if attempt < 3:
            # A raised error is retried per the Workflow's retry_policy.
            raise ApplicationError(
                f"payment gateway timeout (attempt {attempt})", type="GatewayTimeout"
            )
        return f"charged {order_id} on attempt {attempt}"

    @activity.defn
    async def export_large_report(self, pages: int) -> str:
        # On retry, resume from the last recorded heartbeat instead of page 0.
        start_page = 0
        if activity.info().heartbeat_details:
            start_page = activity.info().heartbeat_details[0]
        for page in range(start_page, pages):
            await asyncio.sleep(0.3)
            print(f"exported page {page + 1}/{pages}")
            activity.heartbeat(page + 1)
        return f"exported {pages} pages"


@workflow.defn
class ProcessingWorkflow:
    @workflow.run
    async def process(self, order_id: str) -> str:
        # A deliberate retry policy: 5 attempts, 1s initial backoff doubling each time.
        charge = await workflow.execute_activity_method(
            FlakyActivities.charge_card,
            order_id,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                backoff_coefficient=2.0,
                maximum_attempts=5,
            ),
        )

        # A long Activity: the heartbeat timeout detects a dead Worker between pages.
        exported = await workflow.execute_activity_method(
            FlakyActivities.export_large_report,
            5,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=5),
        )

        return f"{charge} | {exported}"
