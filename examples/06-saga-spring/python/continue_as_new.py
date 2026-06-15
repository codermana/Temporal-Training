from temporalio import workflow

# `has_next_subscription_event` / `handle_next_subscription_event` stand in for
# signal-driven state on the workflow (a Query-able buffer in a real app).


@workflow.defn
class SubscriptionWorkflow:
    # An indefinitely-running, event-driven workflow. After a bounded number of
    # events we call continue_as_new to start a fresh run with a clean history,
    # carrying forward only the state the next run needs.
    @workflow.run
    async def run(self, subscription_id: str, event_count: int) -> None:
        while True:
            await workflow.wait_condition(lambda: self.has_next_subscription_event())
            self.handle_next_subscription_event()
            event_count += 1

            if event_count >= 1000:
                # Does not return — replaces this run with a new one (event_count reset).
                workflow.continue_as_new(args=[subscription_id, 0])
