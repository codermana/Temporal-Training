# Lab 6.5 — Kubernetes + KEDA autoscaling

**Time:** ~70 min · **Difficulty:** ★★★ · **Stack:** kind + KEDA + Temporal

## Scenario

The Worker image from Lab 6.4 now runs on Kubernetes. You'll deploy it with
probes and a deploy strategy that won't kill in-flight Activities, then make it
**autoscale on Task Queue backlog** with KEDA — the right signal for Temporal
Workers (queue depth), not CPU. A local `kind` cluster stands in for EKS.

## Learning goals

- Write a Worker `Deployment` with resource limits, probes, and a safe rolling
  update (`maxUnavailable: 0`, generous `terminationGracePeriodSeconds`).
- Configure a KEDA `ScaledObject` that scales on Temporal Task Queue backlog.
- Observe scale-up as backlog grows and graceful drain on scale-down.

## Prerequisites

```bash
make temporal       # terminal 1 (or run Temporal in-cluster — see note)
make kind-up        # create kind cluster + install KEDA via Helm
make kind-load      # build the Worker image and load it into kind
make kind-status    # cluster + KEDA + ScaledObject state
```

<details><summary>Under the hood — what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
# gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
# Overridable via TEMPORAL_HOST, TEMPORAL_PORT, TEMPORAL_UI_PORT, TEMPORAL_METRICS_PORT.
```

</details>

<details><summary>Under the hood — what <code>make kind-up</code> runs</summary>

```bash
kind create cluster --name temporal-training
kubectl config use-context kind-temporal-training
helm repo add kedacore https://kedacore.github.io/charts && helm repo update
kubectl create namespace keda
helm install keda kedacore/keda --namespace keda --wait
```

</details>

<details><summary>Under the hood — what <code>make kind-load</code> runs</summary>

```bash
docker build -t temporal-transform-worker:dev examples/runnable/08-aws-containers
kind load docker-image temporal-transform-worker:dev --name temporal-training
```

</details>

<details><summary>Under the hood — what <code>make kind-status</code> runs</summary>

```bash
kubectl cluster-info
kubectl get pods -n keda
kubectl get scaledobjects -A
```

</details>

> **Connectivity note:** the in-cluster Worker must reach a Temporal Frontend.
> Easiest for the lab: run Temporal on the host and point the Deployment at
> `host.docker.internal:7233` (or the kind node's gateway). The provided
> `keda-scaledobject.yaml` references an in-cluster
> `temporal-frontend.temporal.svc...` — adjust the endpoint to match wherever
> your Frontend actually runs.

## Starter code

Two manifests to complete. **`k8s-worker-deployment.yaml`:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: temporal-transform-worker
spec:
  replicas: 2
  strategy:
    rollingUpdate:
      maxUnavailable: 0      # preserve capacity/sticky execution during deploys
      maxSurge: 1
  selector:
    matchLabels: { app: temporal-transform-worker }
  template:
    metadata:
      labels: { app: temporal-transform-worker }
    spec:
      terminationGracePeriodSeconds: 120   # TODO: align with startToCloseTimeout
      containers:
        - name: worker
          image: example.com/temporal-transform-worker:latest   # TODO: use your kind-loaded image
          env:
            - name: TEMPORAL_ADDRESS
              valueFrom: { configMapKeyRef: { name: temporal-worker-config, key: temporal-address } }
            - name: TASK_QUEUE
              value: transform
          # TODO: resources.requests/limits (cpu/memory)
          # TODO: readinessProbe + livenessProbe
          #   - if no /health endpoint: exec ["sh","-c","pgrep -f worker.jar"]
          #   - if you added /health in Lab 6.4: httpGet on that port/path
```

**`keda-scaledobject.yaml`:**

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: temporal-transform-worker
spec:
  scaleTargetRef:
    name: temporal-transform-worker
  minReplicaCount: 1          # keep one poller alive
  maxReplicaCount: 10
  triggers:
    - type: temporal          # KEDA's native Temporal scaler polls DescribeTaskQueue
      metadata:
        endpoint: <temporal-frontend-host>:7233   # TODO: match your Frontend
        namespace: default
        taskQueue: transform
        queueType: ActivityTaskQueue
        targetQueueSize: "20"          # TODO: tune
        activationTargetQueueSize: "5"
```

You also need the ConfigMap referenced by the Deployment:

```bash
kubectl create configmap temporal-worker-config \
  --from-literal=temporal-address=host.docker.internal:7233 \
  --from-literal=temporal-namespace=default
```

<details><summary><b>Doing this lab in Python or Go?</b> Manifest scaffolds</summary>

The `Deployment` and `ScaledObject` are **language-neutral** — KEDA's Temporal
scaler watches Task Queue backlog on the server, not the Worker process, so the
same `ScaledObject` works for any image. Two things change per language:

1. **The image** you build and `kind load` (Java fat-JAR vs Python vs Go — see
   the per-language Dockerfiles in
   [`examples/runnable/08-aws-containers`](../../examples/runnable/08-aws-containers)).
2. **The exec probe's process name**, since the Worker is not an HTTP server:

```yaml
# Java
readinessProbe:
  exec: { command: ["sh", "-c", "pgrep -f worker.jar > /dev/null"] }
# Python
readinessProbe:
  exec: { command: ["sh", "-c", "pgrep -f worker.py > /dev/null"] }
# Go (the binary is named "worker")
readinessProbe:
  exec: { command: ["sh", "-c", "pgrep -f '^/worker' > /dev/null"] }
```

Everything else — `maxUnavailable: 0`, `terminationGracePeriodSeconds` aligned
with your longest `startToCloseTimeout`, the KEDA `temporal` trigger on
`taskQueue: transform` with `queueType: ActivityTaskQueue` — is identical across
languages. (Or add an HTTP `/health` endpoint in any language and switch all
three to an `httpGet` probe.)

The rule is identical in all three SDKs: **scale on Task Queue backlog, not CPU**,
and keep Workflow workers at `minReplicaCount ≥ 1` so timers and sticky execution
keep making progress.

</details>

## Tasks

1. Complete the Deployment: image (your kind-loaded tag), resources, probes, and
   `terminationGracePeriodSeconds` aligned with your `startToCloseTimeout`.
2. Apply the ConfigMap and Deployment; confirm pods reach **Ready** and poll the
   `transform` queue.
3. Complete and apply the `ScaledObject`; confirm KEDA registers it.
4. **Generate backlog** and watch it scale up.
5. Let the backlog drain and watch it scale back toward `minReplicaCount`, with
   pods draining gracefully (no mid-Activity kills).

## Verification

```bash
kubectl apply -f k8s-worker-deployment.yaml
kubectl apply -f keda-scaledobject.yaml
kubectl get scaledobject,deployment,pods

# Flood the transform Task Queue to create backlog:
make load-transform N=200          # or: for i in $(seq 1 200); do make start-workflow QUEUE=transform ID=$i; done

# Watch replicas climb as backlog grows:
kubectl get hpa -w                 # KEDA manages an HPA under the hood
kubectl get pods -w
```

<details><summary>Under the hood — what <code>make load-transform</code> runs</summary>

```bash
for i in $(seq 1 200); do
  temporal workflow start --task-queue transform --type ImportWorkflow \
    --workflow-id importworkflow-$i --input "\"s3://imports-incoming/synthetic-$i.csv\""
done
```

</details>

<details><summary>Under the hood — what <code>make start-workflow</code> runs</summary>

```bash
temporal workflow start \
  --task-queue transform \
  --type ImportWorkflow \
  --workflow-id importworkflow-<ID> \
  --input "\"s3://imports-incoming/synthetic-<ID>.csv\""
# TYPE defaults to ImportWorkflow; workflow-id is <type-lowercased>-<ID>.
```

</details>

Expected: replica count rises past 2 toward `maxReplicaCount` while backlog is
high, then settles back to `minReplicaCount` once drained. Deleting a pod during
work shows a graceful drain (`terminationGracePeriodSeconds`), not an abort.

## Definition of done

- [ ] Worker Deployment runs on kind with probes and resource limits.
- [ ] Rolling update uses `maxUnavailable: 0`; grace period ≥ your activity
      timeout.
- [ ] KEDA `ScaledObject` scales replicas on **Task Queue backlog**, not CPU.
- [ ] Backlog growth scales up; drain scales down to `minReplicaCount`.
- [ ] A terminated pod drains in-flight work instead of killing it.

## Pitfalls

- **CPU-based HPA lags** for Temporal Workers — a poller can be idle on CPU while
  a deep backlog waits. Queue depth (KEDA Temporal scaler) is the correct signal.
- **`minReplicaCount: 0`** for Workflow workers means no poller — sticky/timer
  progress stalls. Keep Workflow workers ≥ 1; scale-to-zero only suits pure batch
  Activity pools.
- **Grace period too short** kills Activities mid-run; set
  `terminationGracePeriodSeconds` ≥ the longest `startToCloseTimeout`.
- **Endpoint mismatch** is the most common failure: the `ScaledObject` and the
  Deployment must both reach the *same* reachable Frontend address.

## Hints

<details><summary>Hint 1 — image not found / ImagePullBackOff</summary>

kind nodes don't have your local Docker images unless loaded. `make kind-load`
builds and `kind load docker-image`s it. Set the Deployment `image:` to that
exact tag and `imagePullPolicy: IfNotPresent` so it doesn't try a registry.
</details>

<details><summary>Hint 2 — separate Activity vs Workflow worker pools</summary>

For independent scaling, run two Deployments (different Task Queues / worker
configs): an Activity-worker Deployment that can scale aggressively, and a
Workflow-worker Deployment held at a steady `minReplicaCount`. Two
`ScaledObject`s, one per Deployment.
</details>

## Stretch goals

- Split into separate Activity-worker and Workflow-worker Deployments and scale
  them independently.
- End-to-end: an **S3 event → SQS → Signal bridge** (LocalStack SQS) that starts
  Workflows, so backlog is driven by "files arriving" rather than a manual loop —
  the full *S3 trigger → containerized Worker → S3 output* pipeline.
- Add IRSA-style credential injection (a mounted secret in the lab) so the
  containerized Activities can reach AWS without baked-in keys.
