import asyncio

from temporalio import activity


# Heartbeat on every page. The heartbeat both reports progress (page becomes the
# resume point on retry) and is how the server delivers a cancel: when the
# Workflow cancels, the next await point raises CancelledError here.
@activity.defn
async def export_large_table(table_name: str) -> str:
    for page in range(1000):
        try:
            await export_page(table_name, page)
            activity.heartbeat(page)
        except asyncio.CancelledError:
            await cleanup_partial_export(table_name, page)
            raise
    return f"s3://exports/{table_name}"
