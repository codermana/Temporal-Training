from temporalio import activity

# An idempotent producer lives inside an Activity. enable_idempotence + acks=all
# give exactly-once-per-partition delivery so Temporal's automatic retries can't
# create duplicate outcome events on the broker.


class OutcomeProducerActivity:
    def __init__(self, bootstrap_servers: str):
        from kafka import KafkaProducer

        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            enable_idempotence=True,
            acks="all",
            key_serializer=lambda s: s.encode(),
            value_serializer=lambda s: s.encode(),
        )

    @activity.defn
    async def publish_outcome(self, order_id: str, outcome: str) -> None:
        # Block on the send so a broker failure raises and Temporal retries.
        self._producer.send("order-outcomes", key=order_id, value=outcome).get(timeout=30)
