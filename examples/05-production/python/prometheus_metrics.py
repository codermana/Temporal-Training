from temporalio.client import Client
from temporalio.runtime import (
    PrometheusConfig,
    Runtime,
    TelemetryConfig,
)

# Python has no Micrometer; the SDK's Core runtime exposes a Prometheus endpoint
# directly. Build a Runtime with a PrometheusConfig and pass it to Client.connect.
# The runtime serves Prometheus text on bind_address — no extra HttpServer needed
# (that is the Java micrometer_metrics.java + custom /metrics endpoint, collapsed
# into one config).


async def create_client() -> Client:
    runtime = Runtime(
        telemetry=TelemetryConfig(
            metrics=PrometheusConfig(bind_address="0.0.0.0:9090")
        )
    )
    return await Client.connect("127.0.0.1:7233", runtime=runtime)
