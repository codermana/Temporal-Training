import json

from temporalio import activity

# `db` is a transaction-capable handle (e.g. SQLAlchemy session / connection)
# injected into the Activity instance; `order_repository` / `outbox_repository`
# are repositories that enlist in the same transaction.


@activity.defn
async def write_order_and_outbox(order: dict) -> None:
    # One database transaction: business row and outbox row commit together.
    # A separate relay (or Debezium CDC) later ships the outbox row to Kafka, so
    # the DB write and the event publish can never diverge.
    with db.transaction():
        order_repository.save(order)
        outbox_repository.save(
            topic="order-events",
            key=order["id"],
            payload=json.dumps({"type": "OrderAccepted", "id": order["id"]}),
        )
