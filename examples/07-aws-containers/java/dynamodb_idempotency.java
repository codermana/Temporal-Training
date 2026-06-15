// Activities are at-least-once: a retried or timed-out Activity can run its side
// effect twice. For non-idempotent externals (charge a card, POST to a partner),
// guard with a DynamoDB conditional write keyed by the Activity's idempotency key
// (e.g. the Workflow ID + step). The conditional put is the dedupe; the partner
// call only runs on the first writer.
class DynamoIdempotencyActivitiesImpl implements DynamoIdempotencyActivities {
  private final DynamoDbClient ddb;
  private final String table;

  @Override
  public String chargeOnce(String idempotencyKey, long amountCents) {
    try {
      // Conditional put: succeeds only if this key was never seen before.
      ddb.putItem(
          PutItemRequest.builder()
              .tableName(table)
              .item(
                  Map.of(
                      "pk", AttributeValue.fromS(idempotencyKey),
                      "status", AttributeValue.fromS("IN_PROGRESS")))
              .conditionExpression("attribute_not_exists(pk)")
              .build());
    } catch (ConditionalCheckFailedException duplicate) {
      // A previous attempt already claimed this key — return the stored result
      // instead of charging again.
      return ddb.getItem(
              GetItemRequest.builder()
                  .tableName(table)
                  .key(Map.of("pk", AttributeValue.fromS(idempotencyKey)))
                  .build())
          .item()
          .getOrDefault("result", AttributeValue.fromS(""))
          .s();
    }

    String result = callPaymentPartner(amountCents); // the non-idempotent side effect

    ddb.updateItem(
        UpdateItemRequest.builder()
            .tableName(table)
            .key(Map.of("pk", AttributeValue.fromS(idempotencyKey)))
            .updateExpression("SET #s = :done, #r = :result")
            .expressionAttributeNames(Map.of("#s", "status", "#r", "result"))
            .expressionAttributeValues(
                Map.of(
                    ":done", AttributeValue.fromS("DONE"),
                    ":result", AttributeValue.fromS(result)))
            .build());
    return result;
  }
}
