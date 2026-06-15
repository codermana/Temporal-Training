from temporalio import workflow


# Signal/query handlers are methods on the @workflow.defn class, the Python
# equivalent of @SignalMethod / @QueryMethod. The @workflow.run method blocks in
# workflow.wait_condition until a Signal flips the state (like Workflow.await).
@workflow.defn
class HumanApprovalWorkflow:
    def __init__(self) -> None:
        self._state = "WAITING"

    @workflow.run
    async def run(self, request_id: str) -> str:
        await workflow.wait_condition(lambda: self._state.startswith("APPROVED"))
        return self._state

    @workflow.signal
    def approve(self, approver: str) -> None:
        self._state = "APPROVED by " + approver

    @workflow.query
    def current_state(self) -> str:
        return self._state
