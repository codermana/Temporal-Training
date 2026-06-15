from datetime import timedelta

from temporalio import workflow


# Real-world human-in-the-loop: a document moves through review. Reviewers push
# comments via an Update (synchronous, validated, returns the running count) while
# the final publish/reject decision arrives as a Signal. The run method blocks on
# a single wait_condition that either decision satisfies, with an escalation
# timeout so a stalled review doesn't hang forever.
@workflow.defn
class DocumentReviewWorkflow:
    def __init__(self) -> None:
        self._comments: list[str] = []
        self._decision: str | None = None

    @workflow.run
    async def run(self, doc_id: str) -> str:
        # Auto-reject if no decision lands within the SLA window.
        try:
            await workflow.wait_condition(
                lambda: self._decision is not None,
                timeout=timedelta(days=3),
            )
        except TimeoutError:
            self._decision = "REJECTED: review SLA expired"
        return f"{doc_id} -> {self._decision} ({len(self._comments)} comments)"

    @workflow.update
    def add_comment(self, reviewer: str, comment: str) -> int:
        self._comments.append(f"{reviewer}: {comment}")
        return len(self._comments)

    @add_comment.validator
    def validate_comment(self, reviewer: str, comment: str) -> None:
        if not comment.strip():
            raise ValueError("comment must not be empty")

    @workflow.signal
    def publish(self, approver: str) -> None:
        self._decision = f"PUBLISHED by {approver}"

    @workflow.signal
    def reject(self, reason: str) -> None:
        self._decision = f"REJECTED: {reason}"

    @workflow.query
    def pending_comments(self) -> list[str]:
        return list(self._comments)
