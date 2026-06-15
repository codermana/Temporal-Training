"""Create a daily Schedule that fires DailyReportWorkflow at 09:00.

    python main.py        # needs a Temporal dev server on 127.0.0.1:7233

Mirrors CreateSchedule.java: this is a one-shot client program, not a long-lived
Worker. It registers the Schedule on the server and exits; the server fires the
Workflow on the spec from then on. Run a Worker on the 'reports' task queue
separately if you want the scheduled runs to actually execute.
"""

import asyncio
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleCalendarSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
)

from report import TASK_QUEUE, DailyReportWorkflow

SCHEDULE_ID = "daily-sales-report-schedule"


async def main() -> None:
    client = await Client.connect("127.0.0.1:7233")

    await client.create_schedule(
        SCHEDULE_ID,
        Schedule(
            action=ScheduleActionStartWorkflow(
                DailyReportWorkflow.run,
                "daily-sales",
                id="daily-sales-report",
                task_queue=TASK_QUEUE,
            ),
            spec=ScheduleSpec(
                # 09:00 every day - the calendar equivalent of the Java
                # ScheduleCalendarSpec(hour=9, minute=0).
                calendars=[
                    ScheduleCalendarSpec(
                        hour=[ScheduleRange(9)],
                        minute=[ScheduleRange(0)],
                    )
                ],
            ),
            policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            state=ScheduleState(note="Airflow daily DAG replacement"),
        ),
    )
    print(f"Created Schedule '{SCHEDULE_ID}' (daily at 09:00, overlap=SKIP).")
    print("Inspect it with:  temporal schedule describe --schedule-id " + SCHEDULE_ID)


if __name__ == "__main__":
    asyncio.run(main())
