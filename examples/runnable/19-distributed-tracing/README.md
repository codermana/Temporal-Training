# Distributed Tracing: runnable lab (Java · Python · Go)

A three-step order pipeline (`validate → charge → ship`) instrumented end to
end. The interceptors propagate one trace from the **client** that starts the
Workflow, through the **Workflow** code, and into **each Activity** — so you see
a single connected trace in Jaeger, spanning processes:

```
StartWorkflow:OrderWorkflow            (client / starter)
└─ RunWorkflow:OrderWorkflow           (worker)
   ├─ StartActivity:ValidateOrder  →  RunActivity:ValidateOrder
   ├─ StartActivity:ChargePayment  →  RunActivity:ChargePayment
   └─ StartActivity:ShipOrder      →  RunActivity:ShipOrder
```

The application code is ordinary Temporal code — **nothing** in the Workflow or
Activities mentions tracing. The spans come entirely from interceptors wired in
at the client and Worker, which is the whole point: tracing is cross-cutting.

## Prerequisites

Start a Temporal dev server **and** the Jaeger tracing backend (Jaeger
all-in-one with its OTLP receiver enabled):

```bash
scripts/start-temporal.sh      # or: make temporal
make stack-trace               # Jaeger UI :16686, OTLP :4317 (gRPC) / :4318 (HTTP)
```

The SDKs export spans over OTLP straight to Jaeger — no separate OpenTelemetry
Collector. Override the endpoint with `OTEL_EXPORTER_OTLP_ENDPOINT` if needed.

## Java (`io.temporal:temporal-opentracing` + OpenTelemetry SDK)

```bash
scripts/run-example.sh trace java worker     # terminal 1: Worker (polls forever)
scripts/run-example.sh trace java starter    # terminal 2: starts one Workflow
```

Entry points: `java/.../tracing/TracingWorker.java` and `TracingStarter.java`.
The tracing wiring lives in `Telemetry.java`; Workflow + Activities are
`OrderWorkflowImpl` / `OrderActivitiesImpl`.

## Python (`temporalio` + `opentelemetry-sdk`)

```bash
cd python
uv run worker.py      # terminal 1: Worker
uv run starter.py     # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh trace python worker
#   scripts/run-example.sh trace python starter
```

Entry points: `python/worker.py` and `python/starter.py`; tracing setup in
`python/telemetry.py`, workflow + activities in `python/pipeline.py`.

## Go (`go.temporal.io/sdk/contrib/opentelemetry`)

```bash
cd go
go run ./worker       # terminal 1: Worker
go run ./starter      # terminal 2: starts one Workflow
# or, from the repo root:
#   scripts/run-example.sh trace go worker
#   scripts/run-example.sh trace go starter
```

Entry points: `go/worker/main.go` and `go/starter/main.go`; tracing setup in
`go/telemetry.go`, workflow + activities in `go/order.go` (package `tracing`).

## Expected output

The **starter** prints something like:

```
order A-1001 complete (payment=pay-A-1001, tracking=trk-A-1001)
Open the trace in Jaeger: http://localhost:16686 (service temporal-client-...)
```

Then open the Jaeger UI and find the trace:

```bash
make jaeger        # opens http://localhost:16686
```

Pick the `temporal-client-*` (or `temporal-worker-*`) service, hit **Find
Traces**, and open the most recent one. You'll see the client's start span with
the Workflow and Activity spans nested underneath, each Activity's ~200–500 ms
sleep visible as its span duration.

## OpenTelemetry vs OpenTracing (the migration story)

[OpenTracing](https://opentracing.io/) was the original vendor-neutral tracing
API; it has since been **superseded by [OpenTelemetry](https://opentelemetry.io/)**
(OpenTracing is archived). Temporal's SDKs sit at different points on that
migration, which is exactly why this lab is worth seeing in all three languages:

| SDK | Native tracing module | This lab uses | OpenTracing path |
| --- | --- | --- | --- |
| **Go** | `contrib/opentelemetry` **and** `contrib/opentracing` | OpenTelemetry interceptor | swap the import + `NewTracingInterceptor` for `contrib/opentracing` |
| **Python** | `temporalio.contrib.opentelemetry` only | OpenTelemetry interceptor | none — OpenTelemetry is the only supported path |
| **Java** | `temporal-opentracing` only | OpenTracing interceptors **bridged to** OpenTelemetry | drop the shim; pass a native OpenTracing tracer |

### Java is the interesting case

There is **no official `temporal-opentelemetry` module for Java** — Temporal's
interceptors (`OpenTracingClientInterceptor` / `OpenTracingWorkerInterceptor`)
speak the OpenTracing API. To emit OpenTelemetry spans we keep those exact
interceptors and bridge them with OpenTelemetry's **OpenTracing shim**:

```java
// OpenTelemetry today (this lab) — see Telemetry.java:
OpenTracingOptions otOptions =
    OpenTracingOptions.newBuilder()
        .setTracer(OpenTracingShim.createTracerShim(openTelemetrySdk))  // the bridge
        .build();
new OpenTracingClientInterceptor(otOptions);   // unchanged
new OpenTracingWorkerInterceptor(otOptions);   // unchanged
```

```java
// Legacy plain OpenTracing — the only difference is the tracer you pass:
io.opentracing.Tracer tracer = /* e.g. a Jaeger OpenTracing client */;
OpenTracingOptions otOptions =
    OpenTracingOptions.newBuilder().setTracer(tracer).build();
// ...same two interceptors.
```

So migrating Java from OpenTracing to OpenTelemetry is a **one-line change** to
how the tracer is built; the Temporal-facing wiring is identical. (A community
`temporal-opentelemetry` interceptor library exists if you'd rather avoid the
shim, but the shim is the officially documented approach.)

### Go: a one-import swap

```go
import temporalotel "go.temporal.io/sdk/contrib/opentelemetry"   // this lab
// import temporalot  "go.temporal.io/sdk/contrib/opentracing"   // legacy
i, _ := temporalotel.NewTracingInterceptor(temporalotel.TracerOptions{Tracer: tracer})
clientOpts.Interceptors = []interceptor.ClientInterceptor{i}
```

### Takeaway

For **new** work, use OpenTelemetry in every language. OpenTracing only matters
when you're migrating an existing codebase — and as the table shows, the Temporal
interceptor surface barely changes, so the migration is mostly about swapping the
tracer/exporter, not the Temporal wiring.

> Both APIs export to the **same Jaeger backend** here. Jaeger natively accepts
> OTLP (and historically accepted Jaeger/Zipkin formats), so the backend is
> agnostic to which API your app used.

## Reference

- Teaching snippet: `examples/05-production/{java,go,python}/otel_tracing.*`
- Code Exchange: <https://temporal.io/code-exchange/temporal-opentelemetry>
  (`github.com/temporal-community/temporal-otel`)
