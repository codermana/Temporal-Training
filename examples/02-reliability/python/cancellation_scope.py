import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError

# `export_large_table` is the heartbeating Activity from heartbeat_long_activity.py.


@workflow.defn
class CancellableExportWorkflow:
    # Start a long-running export as a cancellable task, then race it against a
    # deadline. asyncio.wait_for cancels the task when the timeout fires; that
    # cancellation reaches the Activity on its next heartbeat (raising
    # CancelledError there) so it can clean up partial work.
    @workflow.run
    async def export_with_deadline(self, table_name: str, deadline_secs: int) -> str:
        export = workflow.execute_activity(
            export_large_table,
            table_name,
            start_to_close_timeout=timedelta(hours=2),
            heartbeat_timeout=timedelta(seconds=30),
        )
        try:
            return await asyncio.wait_for(export, timeout=deadline_secs)
        except asyncio.TimeoutError:
            raise ApplicationError("export timed out", type="ExportTimeout")
