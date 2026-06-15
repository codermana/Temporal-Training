from temporalio import activity

# A polling consumer lives inside an Activity (it is blocking I/O, never Workflow
# code). kafka-python's KafkaConsumer is created with enable_auto_commit=False so
# we commit only after the Activity's work succeeds — at-least-once into Temporal.


@activity.defn
async def poll_batch(topic: str) -> list[str]:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers="localhost:9092",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b.decode(),
    )
    values: list[str] = []
    try:
        while len(values) < 100:
            for tp, records in consumer.poll(timeout_ms=1000).items():
                for record in records:
                    values.append(record.value)
                    # Heartbeat the offset so a retry resumes from here.
                    activity.heartbeat(f"{record.topic}:{record.partition}:{record.offset}")
        # Commit only after Activity work succeeds.
        consumer.commit()
        return values
    finally:
        consumer.close()
