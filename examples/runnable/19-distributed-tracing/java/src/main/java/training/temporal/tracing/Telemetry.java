package training.temporal.tracing;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.temporal.opentracing.OpenTracingOptions;

/**
 * Wires OpenTelemetry into Temporal for this lab.
 *
 * <p>Temporal's Java tracing interceptors are written against the OpenTracing
 * API ({@code temporal-opentracing}); there is no official temporal-opentelemetry
 * module for Java. To emit OpenTelemetry spans we build a normal OpenTelemetry
 * SDK, expose its {@code Tracer} through the OpenTracing <em>shim</em>, and hand
 * the resulting {@link OpenTracingOptions} to the Temporal interceptors (see
 * {@code TracingWorker} / {@code TracingStarter}). Spans are exported over
 * OTLP/gRPC to Jaeger on :4317.
 *
 * <p><b>Migrating from plain OpenTracing:</b> a legacy setup would build a
 * native OpenTracing tracer (e.g. the Jaeger client) and pass it straight to
 * {@link OpenTracingOptions}. The only change to move to OpenTelemetry is the
 * one line below ({@code OpenTracingShim.createTracerShim(...)}) — the Temporal
 * interceptors are identical either way. See this lab's README.
 */
final class Telemetry {
  private static final String OTLP_ENDPOINT =
      System.getenv().getOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317");

  private Telemetry() {}

  /**
   * Builds the OpenTracing options (backed by OpenTelemetry) that the client and
   * worker interceptors share. Call once per process and reuse for both
   * interceptors so there is a single exporter pipeline.
   */
  static OpenTracingOptions openTracingOptions(String serviceName) {
    OtlpGrpcSpanExporter exporter =
        OtlpGrpcSpanExporter.builder().setEndpoint(OTLP_ENDPOINT).build();

    SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .setResource(
                Resource.getDefault()
                    .merge(
                        Resource.create(
                            Attributes.of(AttributeKey.stringKey("service.name"), serviceName))))
            .build();

    // Critical: a tracer provider alone is not enough. The interceptors inject /
    // extract span context across the client -> Workflow -> Activity boundaries
    // through the shim, which uses the SDK's configured propagator. Without a
    // real propagator the SDK default is a no-op, so every span becomes its own
    // root and the trace fragments. W3C TraceContext stitches them into one.
    OpenTelemetrySdk otel =
        OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();

    // Flush spans to Jaeger on JVM exit (important for the short-lived starter).
    Runtime.getRuntime().addShutdownHook(new Thread(tracerProvider::close));

    // The shim adapts the OpenTelemetry Tracer to the OpenTracing API Temporal expects.
    return OpenTracingOptions.newBuilder()
        .setTracer(OpenTracingShim.createTracerShim(otel))
        .build();
  }
}
