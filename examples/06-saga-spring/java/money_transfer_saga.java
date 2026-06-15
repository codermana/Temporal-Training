// A money-transfer ledger saga: debit the source account, then credit the
// destination. If the credit fails (frozen account, closed, etc.), refund the
// debit so the ledger never loses money. The debit returns a ledger entry id
// that the compensating refund needs — which is why forward steps return ids.
record Transfer(String fromAccount, String toAccount, long amount) {}

@ActivityInterface
interface TransferActivities {
  String debit(Transfer transfer);

  String credit(Transfer transfer);

  void refund(String debitEntryId);
}

class MoneyTransferSaga {
  private final TransferActivities activities =
      Workflow.newActivityStub(
          TransferActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(30)).build());

  String transfer(Transfer t) {
    // Saga collects compensations as it goes, then runs them in reverse on failure.
    Saga saga = new Saga(new Saga.Options.Builder().build());
    try {
      String debitEntry = activities.debit(t);
      saga.addCompensation(activities::refund, debitEntry);

      activities.credit(t);
      return "SETTLED";
    } catch (ActivityFailure e) {
      saga.compensate();
      return "REVERSED";
    }
  }
}
