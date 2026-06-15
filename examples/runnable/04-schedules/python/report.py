"""The Workflow a Schedule fires. The Python equivalent of DailyReportWorkflow.java.

Kept trivial on purpose: the lab is about the Schedule, not the report. Run a
Worker on TASK_QUEUE if you want the scheduled runs to actually execute.
"""

from temporalio import workflow

TASK_QUEUE = "reports"


@workflow.defn
class DailyReportWorkflow:
    @workflow.run
    async def run(self, report_name: str) -> None:
        workflow.logger.info("building report %s", report_name)
