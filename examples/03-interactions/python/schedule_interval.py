from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)


# A Schedule is a server-managed cron replacement. The interval spec fires every
# fixed period; jitter spreads the fire time to avoid thundering herds, and the
# overlap policy decides what happens when a run is still going as the next fires.
async def create_hourly_schedule(client: Client) -> None:
    await client.create_schedule(
        "hourly-orders",
        Schedule(
            action=ScheduleActionStartWorkflow(
                OrdersWorkflow.run,
                "hourly",
                id="hourly-orders-wf",
                task_queue="orders",
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
                jitter=timedelta(minutes=5),
            ),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.BUFFER_ONE),
        ),
    )
