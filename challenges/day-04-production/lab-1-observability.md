# Lab 4.1 — Observability: metrics & dashboards

**Time:** ~50 min · **Difficulty:** ★★ · **Stack:** Temporal + Prometheus + Grafana

## Scenario

You can't operate what you can't see. You'll wire the Java SDK's metrics into a
Prometheus registry, expose them, let Prometheus scrape your Worker, view the
pre-loaded Temporal dashboard in Grafana, and add one **custom** Activity metric
of your own.

## Learning goals

- Emit SDK metrics via Micrometer + a Prometheus registry
  (`MicrometerClientStatsReporter`).
- Expose a `/metrics` endpoint Prometheus can scrape.
- Read the Temporal overview dashboard in Grafana.
- Add a custom counter/histogram from inside an Activity.

## Prerequisites

```bash
make temporal      # terminal 1
make stack-obs     # terminal 2: Prometheus :9091 + Grafana :3000
```

<details><summary>Under the hood — what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
# gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
# Overridable via env: TEMPORAL_HOST, TEMPORAL_PORT, TEMPORAL_UI_PORT, TEMPORAL_METRICS_PORT.
```

</details>

<details><summary>Under the hood — what <code>make stack-obs</code> runs</summary>

```bash
docker compose -f docker/compose.observability.yml up -d
# Observability stack: Prometheus :9091 + Grafana :3000.
```

</details>

The Prometheus scrape config (`docker/observability/prometheus.yml`) already
targets a Worker metrics endpoint, and Grafana has a pre-provisioned Temporal
dashboard (`docker/observability/grafana/dashboards/temporal-overview.json`).
Check which host/port Prometheus expects to scrape and expose your Worker's
metrics there.

## Starter code

Reuse any prior Worker (the Day 2 pricing or approval module is ideal). Add
Micrometer + Prometheus to the `pom.xml`:

```xml
<dependency>
  <groupId>io.micrometer</groupId>
  <artifactId>micrometer-registry-prometheus</artifactId>
  <version>1.13.0</version>
</dependency>
```

Wire the SDK to report through Micrometer when you build the service stubs:

```java
// In your Worker main, BEFORE creating WorkflowServiceStubs:
PrometheusMeterRegistry registry =
    new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

// TODO 1: build a StatsReporter from the registry:
//   StatsReporter reporter = new MicrometerClientStatsReporter(registry);
//   Scope scope = new RootScopeBuilder().reporter(reporter)
//       .reportEvery(com.uber.m3.util.Duration.ofSeconds(1));
// TODO 2: pass the scope into WorkflowServiceStubsOptions.newBuilder().setMetricsScope(scope)
// TODO 3: expose registry.scrape() over HTTP on the path/port Prometheus scrapes
//   (a tiny com.sun.net.httpserver.HttpServer on /metrics is enough).
```

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference snippets: [`examples/05-production/python`](../../examples/05-production/python)
(`prometheus_metrics.py`, `custom_activity_metric.py`) and
[`.../go`](../../examples/05-production/go) (`prometheus_metrics.go`,
`custom_activity_metric.go`). Neither SDK uses Micrometer — the Core runtime
exposes Prometheus directly.

**Python** (`temporalio`) — the runtime serves `/metrics` itself; no `HttpServer`:

```python
from temporalio.client import Client
from temporalio.runtime import Runtime, TelemetryConfig, PrometheusConfig
from temporalio import activity

async def make_client() -> Client:
    # TODO 1: build a Runtime whose TelemetryConfig.metrics is a PrometheusConfig
    #         bound to the host/port Prometheus scrapes.
    # TODO 2: pass runtime=... into Client.connect — SDK metrics now flow.
    runtime = Runtime(telemetry=TelemetryConfig(
        metrics=PrometheusConfig(bind_address="0.0.0.0:9090")))
    return await Client.connect("127.0.0.1:7233", runtime=runtime)

@activity.defn
async def price(sku: str) -> int:
    # TODO 3: custom metric — same runtime, queryable in Prometheus.
    activity.metric_meter().create_counter("orders_priced_total").add(1)
    return 0
```

**Go** (`go.temporal.io/sdk`) — set a `client.MetricsHandler` (built from the
`go.temporal.io/sdk/contrib/tally` module + a Prometheus reporter):

```go
import (
    "go.temporal.io/sdk/activity"
    "go.temporal.io/sdk/client"
)

func makeClient(handler client.MetricsHandler) (client.Client, error) {
    // TODO 1: build handler from sdktally.NewMetricsHandler(scope) where scope
    //         uses a tally Prometheus reporter on the scraped port.
    // TODO 2: pass it as client.Options{MetricsHandler: handler}.
    return client.Dial(client.Options{HostPort: "127.0.0.1:7233", MetricsHandler: handler})
}

func Price(ctx context.Context, sku string) (int, error) {
    // TODO 3: custom metric via the activity's handler.
    activity.GetMetricsHandler(ctx).Counter("orders_priced_total").Inc(1)
    return 0, nil
}
```

The principle is the same in all three: attach the metrics sink at the
**client/runtime** level (not the worker), then read your custom Activity metric
in Prometheus. In Go the resource-based tuner is not exposed; size slots manually.

</details>

## Tasks

1. Add the dependency and wire `MicrometerClientStatsReporter` into the service
   stubs' metrics scope.
2. Expose `/metrics` (plain `HttpServer`) on the port Prometheus expects.
3. Run the Worker and generate some Workflow load.
4. Confirm Prometheus is scraping you; open the Grafana dashboard.
5. **Add a custom metric** in an Activity — e.g. a counter
   `orders_priced_total` or a timer around the work — and watch it appear in
   Prometheus.

## Verification

```bash
# Your Worker's metrics endpoint returns Prometheus text
curl -s localhost:<your-metrics-port>/metrics | head

# Prometheus sees the target as UP
open http://127.0.0.1:9091/targets        # (or visit in a browser)

# Your custom metric shows up
curl -s localhost:9091/api/v1/query --data-urlencode 'query=orders_priced_total'
```

In Grafana (<http://127.0.0.1:3000>, admin/admin) open the **Temporal Overview**
dashboard and watch panels move as you drive Workflow load.

## Definition of done

- [ ] Worker exposes SDK metrics at `/metrics` in Prometheus format.
- [ ] Prometheus lists the Worker target as **UP**.
- [ ] The Grafana Temporal dashboard renders live data.
- [ ] A custom Activity metric is queryable in Prometheus.

## Pitfalls

- **Scrape mismatch.** If Prometheus shows the target `DOWN`, your endpoint
  host/port doesn't match `docker/observability/prometheus.yml`. From inside the
  Prometheus container, your laptop is `host.docker.internal` (macOS) — check the
  provided config before changing it.
- **Metrics scope vs. client.** The scope must be attached to the
  `WorkflowServiceStubs` options, not the `WorkflowClient`, or SDK metrics won't
  flow.
- Don't block the Worker thread serving `/metrics`; run the HTTP server on its
  own executor.

## Hints

<details><summary>Hint 1 — minimal metrics endpoint</summary>

```java
HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
server.createContext("/metrics", ex -> {
  byte[] body = registry.scrape().getBytes(StandardCharsets.UTF_8);
  ex.sendResponseHeaders(200, body.length);
  try (var os = ex.getResponseBody()) { os.write(body); }
});
server.start();
```
</details>

<details><summary>Hint 2 — custom metric from an Activity</summary>

Hold a reference to the same `PrometheusMeterRegistry` (or `Metrics.globalRegistry`)
and `registry.counter("orders_priced_total").increment();` inside the Activity
impl. Activities are normal Java — no determinism limits apply.
</details>

## Stretch goals

- Add an **OpenTelemetry** tracing interceptor (`OpenTracingClientInterceptor`)
  and confirm spans propagate from Workflow into Activities.
- Add a histogram for Activity latency and build a Grafana panel for its p95.
