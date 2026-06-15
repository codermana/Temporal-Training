import asyncio
from datetime import timedelta

from temporalio import workflow

# `send`, `ask_primary`, `ask_fallback` are module-level @activity.defn functions.


@workflow.defn
class AsyncProcedureAndRaceWorkflow:
    # Fan a void Activity out across many ids without blocking between sends.
    # asyncio.gather is the equivalent of Promise.allOf — wait for every branch.
    @workflow.run
    async def notify_everyone(self, user_ids: list[str]) -> None:
        opts = dict(start_to_close_timeout=timedelta(minutes=1))
        sends = [workflow.execute_activity(send, uid, **opts) for uid in user_ids]
        await asyncio.gather(*sends)

    # asyncio.FIRST_COMPLETED is the equivalent of Promise.anyOf: continue as
    # soon as the FIRST branch finishes (primary vs fallback provider).
    async def first_to_answer(self, query: str) -> str:
        opts = dict(start_to_close_timeout=timedelta(minutes=1))
        primary = asyncio.ensure_future(workflow.execute_activity(ask_primary, query, **opts))
        fallback = asyncio.ensure_future(workflow.execute_activity(ask_fallback, query, **opts))
        done, _ = await workflow.wait(
            [primary, fallback], return_when=asyncio.FIRST_COMPLETED
        )
        return next(iter(done)).result()
