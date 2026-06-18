# AWS import pipeline + container Worker: runnable lab (Java · Python · Go)

The same `ImportWorkflow` (validate → transform → load) in three SDKs, plus the
container/Kubernetes assets that take that Worker to production. The teaching
point is identical everywhere: *a Worker is a stateless, outbound-only process,
so it containerizes cleanly and scales on Task Queue backlog, not CPU.*

All three connect to a local dev server. Start one first:

```bash
scripts/start-temporal.sh      # or: make temporal
```

Activities pass S3 **URIs** forward, never file bytes, so Workflow history stays
small. The bodies fake the work (sleep + URI rewriting); in the Day 6 labs they
call LocalStack S3. (Glue is a paid-tier LocalStack emulator, so the supervised
"Glue job" is a self-hosted local runner doing real S3 work — see
[`17-spring-glue-pipeline`](../17-spring-glue-pipeline/).)

## Java (`io.temporal:temporal-sdk`)

```bash
cd java && mvn -q compile exec:java
```

Entry point: `java/src/main/java/training/temporal/aws/WorkerMain.java`
(env-driven: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TASK_QUEUE`).

## Python (`temporalio`)

```bash
cd python
uv run worker.py
```

Entry point: `python/worker.py` (workflow + activities in `python/import_pipeline.py`).

## Go (`go.temporal.io/sdk`)

```bash
cd go
go run .
```

Entry point: `go/main.go` (workflow + activities in `go/import_pipeline.go`).

## Start a Workflow

Once any one Worker is polling `transform`:

```bash
temporal workflow start \
  --task-queue transform --type ImportWorkflow \
  --workflow-id import-1 \
  --input '"s3://imports-incoming/incoming/orders.csv"'
```

Expected result: `s3://imports-incoming/transformed/orders.csv?rows=<n>`.

## Containers

Each language has its own `Dockerfile` (Java fat-JAR vs `uv sync` vs
`go build`) because the build differs, but they all produce the *same* kind of
artifact: a Worker that dials **out** to the Frontend, with no inbound port.

```bash
# Java
docker build -t temporal-transform-worker:dev ./java
# Python
docker build -t temporal-transform-worker:dev ./python
# Go
docker build -t temporal-transform-worker:dev ./go

docker run --rm \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  -e TASK_QUEUE=transform \
  temporal-transform-worker:dev
```

## Kubernetes + KEDA

`k8s-worker-deployment.yaml` and `keda-scaledobject.yaml` are **language-neutral**:
the same Deployment and KEDA `ScaledObject` work regardless of which image you
built above (just set `image:` to your tag). KEDA's native Temporal scaler scales
replicas on Task Queue backlog (`DescribeTaskQueue`), the correct signal for
Workers, since a poller can be idle on CPU while a deep backlog waits.

```bash
kubectl apply -f k8s-worker-deployment.yaml
kubectl apply -f keda-scaledobject.yaml
```

The Java Deployment probes with `pgrep -f worker.jar`; for Python/Go adjust the
exec probe to match the process name (`pgrep -f worker.py` / `pgrep -f worker`),
or add an HTTP `/health` endpoint and switch to an `httpGet` probe.
```
