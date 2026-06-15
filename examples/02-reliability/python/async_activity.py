from datetime import timedelta

from temporalio import workflow

# `extract`, `transform`, `load` are module-level @activity.defn functions
# (omitted here for clarity). For activities defined as class methods instead,
# use workflow.start_activity_method / execute_activity_method.


# In Python the Workflow runs on asyncio. start_activity() returns an awaitable
# handle right away; await it to get the result. Kicking off two extracts before
# awaiting either lets them run concurrently — the Async.function equivalent.
@workflow.defn
class AsyncWorkflow:
    @workflow.run
    async def run(self, batch_date: str) -> None:
        opts = dict(start_to_close_timeout=timedelta(minutes=5))

        raw = workflow.start_activity(extract, batch_date, **opts)
        audit = workflow.start_activity(extract, f"{batch_date}-audit", **opts)

        clean_uri = await workflow.execute_activity(transform, await raw, **opts)
        clean_audit_uri = await workflow.execute_activity(transform, await audit, **opts)

        await workflow.execute_activity(load, clean_uri, **opts)
        await workflow.execute_activity(load, clean_audit_uri, **opts)
