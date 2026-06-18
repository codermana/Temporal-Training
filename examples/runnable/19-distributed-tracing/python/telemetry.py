"""OpenTelemetry setup for the tracing lab.

Python has a native OpenTelemetry interceptor
(``temporalio.contrib.opentelemetry.TracingInterceptor``), so there is no
OpenTracing shim here, unlike the Java lab. The Temporal Python SDK does not
ship an OpenTracing integration at all — OpenTelemetry is the only path. See the
lab README.
"""

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from temporalio.contrib.opentelemetry import TracingInterceptor

# Jaeger's OTLP/HTTP receiver. Override with OTEL_EXPORTER_OTLP_ENDPOINT (the
# OTLP/HTTP traces path is <endpoint>/v1/traces).
_OTLP_ENDPOINT = os.environ.get(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"
).rstrip("/")


def init_tracer(service_name: str) -> TracerProvider:
    """Register a global TracerProvider that exports OTLP/HTTP to Jaeger.

    Returns the provider so callers can ``provider.shutdown()`` to flush spans
    before a short-lived process (the starter) exits.
    """
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    exporter = OTLPSpanExporter(endpoint=f"{_OTLP_ENDPOINT}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider


def tracing_interceptor() -> TracingInterceptor:
    """The interceptor to pass to Client.connect(interceptors=[...])."""
    return TracingInterceptor()
