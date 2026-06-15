"""Continue-as-new counter: process work in bounded batches per run.

The Python equivalent of the Java CounterWorkflow lab. Each run processes a small
batch, then calls workflow.continue_as_new to start a fresh run with a clean
history — carrying forward only what the next run needs (processed_so_far).
"""

from datetime import timedelta

from temporalio import workflow

TASK_QUEUE = "continue-as-new"

# Cap the work done in a single run so history stays small...
BATCH_PER_RUN = 3
# ...and stop continuing once the whole job is done.
TOTAL = 9


@workflow.defn
class CounterWorkflow:
    @workflow.run
    async def count(self, processed_so_far: int) -> str:
        processed = processed_so_far
        this_run = 0

        while this_run < BATCH_PER_RUN and processed < TOTAL:
            # Durable timer (never sleep on the wall clock in workflow code).
            await workflow.sleep(timedelta(milliseconds=500))
            processed += 1
            this_run += 1
            workflow.logger.info(
                "processed %d of %d (this run: %d)", processed, TOTAL, this_run
            )

        if processed >= TOTAL:
            return f"completed after {processed} iterations"

        # Does not return — starts a fresh run carrying only processed forward.
        workflow.continue_as_new(processed)
