from datetime import timedelta

from temporalio import workflow

# `load` is a module-level @activity.defn function (does the actual I/O).


@workflow.defn
class ReplaySafeWorkflow:
    @workflow.run
    async def run(self, batch_date: str) -> None:
        # Temporal records these deterministic decisions in Workflow history.
        now = workflow.now()                 # replay-safe time source
        shard = workflow.random().randint(0, 9)  # replay-safe, seeded RNG

        # I/O belongs in Activities because Activity results are recorded.
        await workflow.execute_activity(
            load,
            f"s3://bucket/clean/{batch_date}/shard={shard}?ts={now.timestamp()}",
            start_to_close_timeout=timedelta(minutes=10),
        )
