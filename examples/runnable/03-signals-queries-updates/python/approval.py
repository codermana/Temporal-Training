"""Approval workflow: signal, query, and update on one running execution.

The Python equivalent of the Java ApprovalWorkflow lab. The Workflow blocks in
workflow.wait_condition until an approve/reject Signal arrives, so a Query and an
Update run against a live execution.
"""

from temporalio import workflow

TASK_QUEUE = "approval"
WORKFLOW_ID = "approval-demo"


@workflow.defn
class ApprovalWorkflow:
    def __init__(self) -> None:
        self._request_id = ""
        self._status = "WAITING"
        self._note = "initial request"

    @workflow.run
    async def run(self, request_id: str) -> str:
        self._request_id = request_id
        await workflow.wait_condition(
            lambda: self._status.startswith("APPROVED")
            or self._status.startswith("REJECTED")
        )
        return self._state()

    @workflow.signal
    def approve(self, approver: str) -> None:
        self._status = "APPROVED by " + approver

    @workflow.signal
    def reject(self, reason: str) -> None:
        self._status = "REJECTED: " + reason

    @workflow.query
    def current_state(self) -> str:
        return self._state()

    @workflow.update
    def change_note(self, note: str) -> str:
        self._note = note
        return self._state()

    @change_note.validator
    def validate_note(self, note: str) -> None:
        if not note or not note.strip():
            raise ValueError("note is required")

    def _state(self) -> str:
        return f"{self._request_id} {self._status} ({self._note})"
