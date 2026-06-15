from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)


# Airflow schedule_interval + catchup map directly onto a Temporal Schedule.
async def create_daily_cron(client: Client) -> None:
    await client.create_schedule(
        "daily-orders-cron",
        Schedule(
            action=ScheduleActionStartWorkflow(
                OrdersWorkflow.run,
                "daily",
                id="daily-orders-wf",
                task_queue="orders",
            ),
            spec=ScheduleSpec(
                # schedule_interval -> cron strings, intervals, OR calendars.
                cron_expressions=["0 9 * * *"],  # 09:00 every day
                jitter=timedelta(minutes=5),
            ),
            policy=SchedulePolicy(
                # catchup -> bounded window in which missed runs are fired.
                catchup_window=timedelta(hours=1),
                overlap=ScheduleOverlapPolicy.SKIP,
            ),
        ),
    )


# Overlap policy = what happens when a run is still going when the next fires:
#   SKIP            - drop the new run (Airflow max_active_runs=1, catchup off)
#   BUFFER_ONE      - queue exactly one to run next
#   BUFFER_ALL      - queue every missed run
#   ALLOW_ALL       - run them concurrently
#   CANCEL_OTHER    - cancel the running one, then start
#   TERMINATE_OTHER - terminate the running one, then start
