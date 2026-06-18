# 18 · AWS import pipeline: the runnable Day-6 labs (Java)

A single plain-SDK Java Worker that makes the **Day-6 morning labs actually
compile and run** against LocalStack — real S3 reads/writes, a supervised
transform job, an SQS event trigger, an SNS fan-out notification, and config +
a secret from SSM Parameter Store. Where [`08-aws-containers`](../08-aws-containers/)
*fakes* the I/O (sleeps + URI rewriting) to focus on the container story, this
module does the **real LocalStack work** the labs describe, end to end.

> **Java only**, plain SDK (no Spring) — it mirrors the labs' own scaffolds
> (`WorkerMain` builds the stubs/factory by hand; `S3Client`/`SqsClient`/
> `SnsClient`/`SsmClient` are pointed at LocalStack in [`AwsClients`](java/src/main/java/training/temporal/aws/AwsClients.java)).
> The Spring-Boot take on the same AWS surface is [`17-spring-glue-pipeline`](../17-spring-glue-pipeline/).

```
  file lands ─▶ S3 imports-incoming/orders.csv
      │
      └─drop event─▶ SQS imports-events ──┐
                                          │ (or start the Workflow directly)
                       ┌──────────────────▼─────────────────────────────────┐
                       │ WorkerMain (one process):                          │
                       │   SqsSignalBridge ──signalWithStart──▶             │
                       │        ImportWorkflow:                             │
                       │          1. validate        (S3 read → validated)  │
                       │          2. transform       (supervised job, S3)   │
                       │          3. load            (S3 read → row count)   │
                       │          4. publishNotification (SNS publish)       │
                       └──────────────────────────┬─────────────────────────┘
                                                  │
                          SNS imports-complete ───▼ fan-out
                                          SQS imports-complete-sub
```

## Which lab is which file

| Lab | What it teaches | Where it lives here |
|---|---|---|
| 6.1 Glue activity | start → poll + **heartbeat** → map terminal state | [`TransformJobRunner`](java/src/main/java/training/temporal/aws/TransformJobRunner.java) + `ImportActivitiesImpl.transform` |
| 6.2 S3 checkpointing | pass S3 **URIs**, not bytes; references in history | `ImportActivitiesImpl` (validate/transform/load), [`S3Uri`](java/src/main/java/training/temporal/aws/S3Uri.java) |
| 6.3 Step Functions migration | state machine → one Workflow; Retry→RetryOptions; Catch→try/catch | [`ImportWorkflowImpl`](java/src/main/java/training/temporal/aws/ImportWorkflowImpl.java) |
| 6.4 Containerize the Worker | multi-stage build, container JVM, graceful drain | [`Dockerfile`](java/Dockerfile), `WorkerMain` shutdown hook |
| 6.5 Kubernetes + KEDA | Deployment + scale on Task Queue backlog | [`k8s-worker-deployment.yaml`](k8s-worker-deployment.yaml), [`keda-scaledobject.yaml`](keda-scaledobject.yaml) |
| 6.6 SQS event trigger | `signalWithStart`, idempotent on Workflow ID, delete-after-durable | [`SqsSignalBridge`](java/src/main/java/training/temporal/aws/SqsSignalBridge.java) |
| 6.7 SNS fan-out notify | publish from an **Activity**, `workflowId` for dedup | `ImportActivitiesImpl.publishNotification` |
| 6.8 SSM config & secret | config at startup; secret read **at use-time** in an Activity | [`WorkerBootstrap`](java/src/main/java/training/temporal/aws/WorkerBootstrap.java), [`SecretActivities`](java/src/main/java/training/temporal/aws/SecretActivities.java), `Secrets` |

## Run it

Three terminals' worth of setup (LocalStack is free-tier; Glue is never called):

```bash
make temporal        # 1) Temporal dev server on :7233 / UI :8233
make stack-aws       # 2) LocalStack (S3/SQS/SNS/SSM) on :4566
make seed-import     # 3) buckets + SQS bus + SNS topic & subscriber + SSM config/secret + an input CSV
```

Then start the Worker (also runs the SQS bridge in-process):

```bash
make run-import
# or: scripts/run-example.sh import
# or: cd java && mvn -q compile exec:java
```

The Worker loads its Temporal address / namespace / task-queue from **SSM** at
startup (Lab 6.8), so you'll see `Worker started. target=127.0.0.1:7233 ... bridge=true`.

### Drive it — the direct path

```bash
temporal workflow start \
  --task-queue transform --type ImportWorkflow \
  --workflow-id import-orders --input '"s3://imports-incoming/orders.csv"'

temporal workflow result --workflow-id import-orders
# "s3://imports-output/orders.csv?rows=3"
```

### Drive it — the event path (Lab 6.6)

```bash
scripts/seed-import-demo.sh trigger          # drop a file-arrival message on the SQS bus
scripts/seed-import-demo.sh notify           # read the SNS notification from the subscriber (Lab 6.7)
scripts/seed-import-demo.sh ls               # see incoming/validated/output objects in S3 (Lab 6.2)

# Idempotency: re-send the SAME file while a run is in flight → one run, triggerCount climbs:
for i in 1 2 3; do scripts/seed-import-demo.sh trigger; done
temporal workflow query --workflow-id import-orders --type getTriggerCount
```

Confirm the SSM tree (the SecureString decrypts) with:

```bash
make aws-resources    # or: awslocal ssm get-parameters-by-path --path /temporal-training/worker/ --with-decryption
```

## Container & Kubernetes (Labs 6.4–6.5)

```bash
docker build -t temporal-import-worker:dev ./java          # Lab 6.4

# Lab 6.5 (needs kind + kubectl + helm, e.g. make kind-up):
kind load docker-image temporal-import-worker:dev --name temporal-training
kubectl apply -f k8s-worker-deployment.yaml
kubectl apply -f keda-scaledobject.yaml
```

The Deployment and `ScaledObject` scale replicas on **Task Queue backlog**
(KEDA's native Temporal scaler), the correct signal for Workers. Set the
ConfigMap's `temporal-address` / `aws-endpoint` to whatever the pods can reach.

## Running against real AWS

Nothing about the Workflow changes — only the edges:

1. **Clear `AWS_ENDPOINT`** (unset it). The clients then use the default endpoint
   resolver and the `DefaultCredentialsProvider` chain — the ECS task role or EKS
   IRSA — so there are no static keys in the image (Labs 6.8–6.10).
2. **Use a real Glue job** if you want managed Spark: implement `TransformJobRunner`'s
   two methods over `GlueClient` (`startJobRun` → `StartJobRun`, `getJobRun` →
   `GetJobRun`). The supervise loop in `ImportActivitiesImpl.transform` is unchanged.
3. **Point at your real bucket / queue / topic / parameter names** via the
   `BUCKET_*`, `IMPORTS_EVENTS_QUEUE_URL`, `NOTIFY_TOPIC_ARN`, and `SSM_*` env vars
   (see [`Config`](java/src/main/java/training/temporal/aws/Config.java)).
