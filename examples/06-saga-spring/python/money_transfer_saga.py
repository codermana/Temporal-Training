from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

# A money-transfer ledger saga: debit the source account, then credit the
# destination. If the credit fails (frozen account, closed, etc.), refund the debit
# so the ledger never loses money. The debit returns a ledger entry id that the
# compensating refund needs — which is why forward steps return ids.


@dataclass
class Transfer:
    from_account: str
    to_account: str
    amount: int


@activity.defn
async def debit(transfer: Transfer) -> str:
    return f"debit-{transfer.from_account}-{transfer.amount}"


@activity.defn
async def credit(transfer: Transfer) -> str:
    if transfer.to_account.lower().startswith("frozen"):
        raise RuntimeError("destination account is frozen")
    return f"credit-{transfer.to_account}-{transfer.amount}"


@activity.defn
async def refund(debit_entry_id: str) -> None:
    activity.logger.info("refunded ledger entry %s", debit_entry_id)


@workflow.defn
class MoneyTransferSaga:
    @workflow.run
    async def transfer(self, t: Transfer) -> str:
        opts = dict(start_to_close_timeout=timedelta(seconds=30))
        compensations: list = []
        try:
            debit_entry = await workflow.execute_activity(debit, t, **opts)
            compensations.append((refund, debit_entry))

            await workflow.execute_activity(credit, t, **opts)
            return "SETTLED"
        except Exception:
            for compensate, arg in reversed(compensations):
                await workflow.execute_activity(compensate, arg, **opts)
            return "REVERSED"
