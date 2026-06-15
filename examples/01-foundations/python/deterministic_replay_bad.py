import random
import time
from datetime import datetime

from temporalio import workflow


@workflow.defn
class BadReplayWorkflow:
    @workflow.run
    async def run(self, batch_date: str) -> None:
        # Bad: replay re-runs Workflow code. This value changes on replay.
        now = time.time()

        # Bad: this random value is not recorded in Workflow history.
        shard = random.randint(0, 9)

        # Bad: direct I/O in Workflow code can run again during replay.
        with open("/tmp/workflow.log", "w") as f:
            f.write(f"{now}:{shard}")
        # (datetime.now() is just as unsafe — never read wall-clock here.)
        _ = datetime.now()
