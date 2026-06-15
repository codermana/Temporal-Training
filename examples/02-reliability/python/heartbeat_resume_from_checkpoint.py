from temporalio import activity

# `copy_page` is an unshown helper that copies one page of the dataset.


# Real-world: a multi-hour data backfill that must resume where it left off after
# a worker crash or activity retry — not restart from page 0. The heartbeat
# carries the last completed page; on a retry the next attempt reads it back
# (activity.info().heartbeat_details) and skips the work already done.
@activity.defn
async def backfill(dataset: str) -> str:
    details = activity.info().heartbeat_details
    start_page = details[0] if details else 0  # resume point, 0 on first attempt

    for page in range(start_page, 100_000):
        await copy_page(dataset, page)
        activity.heartbeat(page)  # checkpoint: this page is done
    return f"backfilled {dataset}"
