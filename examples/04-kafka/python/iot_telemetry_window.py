from datetime import timedelta

from temporalio import workflow

# `publish_rollup` is a module-level @activity.defn function.


@workflow.defn
class TelemetryWindowWorkflow:
    def __init__(self) -> None:
        self._readings: list[float] = []

    # One long-lived Workflow per IoT device. Each Kafka telemetry record arrives
    # as a Signal; the Workflow buffers readings and, every fixed window, publishes
    # a rollup to a `device-rollups` topic. workflow.sleep is the replay-safe timer
    # that closes the window — never asyncio.sleep / time.sleep.
    @workflow.run
    async def run(self, device_id: str) -> None:
        windows = 0
        while True:
            await workflow.sleep(timedelta(minutes=1))
            if self._readings:
                avg = sum(self._readings) / len(self._readings)
                await workflow.execute_activity(
                    publish_rollup,
                    args=["device-rollups", device_id, avg, len(self._readings)],
                    start_to_close_timeout=timedelta(seconds=15),
                )
                self._readings.clear()
            windows += 1
            if windows >= 60:
                workflow.continue_as_new(device_id)

    @workflow.signal
    def on_reading(self, value: float) -> None:
        self._readings.append(value)
