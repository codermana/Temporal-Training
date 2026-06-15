class PaymentCaptureWorkflow {
  private final PaymentActivities payments =
      Workflow.newActivityStub(
          PaymentActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(4).build())
              .build());

  // Driven by a `payment-authorized` Kafka event (bridged in via signalWithStart).
  // Capture the funds, then publish a settlement event. If publishing the result
  // fails after the money moved, run a compensating refund so the system never
  // ends in a "captured but never reported" state — a Kafka-fed saga.
  void run(String paymentId, long amountCents) {
    String captureRef = payments.capture(paymentId, amountCents);
    try {
      payments.publishSettlement("payment-settlements", paymentId, captureRef);
    } catch (ActivityFailure publishFailed) {
      payments.refund(paymentId, captureRef);
      throw ApplicationFailure.newFailure(
          "settlement publish failed, refunded " + paymentId, "SettlementPublishFailed");
    }
  }
}
