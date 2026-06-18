# 17 · Spring Boot orchestrating an AWS Glue job (the full loop)

A runnable Spring Boot service that ties Day 5 (the `temporal-spring-boot-starter`)
to Day 6 (Glue / S3 / SNS / SQS) into one picture: **producer A lands Parquet in
S3 and drops a message on a bus; Temporal validates the data, triggers and
supervises a Glue job that stitches it, then notifies A back.**

```
  Producer A ──writes Parquet──▶  S3  (s3://lake/raw/orders/dt=…/part-*.parquet)
      │
      └──drops event──▶  SQS "glue-triggers"  (the communication bus)
                              │
                  ┌───────────▼─────────────────────────────────────────┐
                  │  Spring Boot + Temporal (this app, one process)      │
                  │   SqsTriggerBridge  ──signalWithStart──▶             │
                  │                          GlueStitchWorkflow:         │
                  │     1. validatePartition  (list S3, assert non-empty)│
                  │     2. runGlueJob         (start + poll Glue*)       │
                  │     3. publishValidation  (SNS publish)              │
                  └───────────────────────────────┬─────────────────────┘
                                                   │
                              SNS "lake-validated" ▼ fan-out
                                          SQS  ──▶ Producer A is notified
```

> \* Glue is **Pro-only on LocalStack Community**, so the app supervises a
> **faked** Glue job (`FakeGlueJobRunner`) that mimics `StartJobRun` + poll →
> `SUCCEEDED` with heartbeats. The Workflow, Activity, retry, and heartbeat
> semantics are exactly what a real `GlueClient`-backed runner uses — swapping it
> in for real AWS is a one-class change (see [below](#running-against-real-aws)).

> **Java only**, like Day 5's [`16-spring-boot`](../16-spring-boot/): the starter
> is a Java/Spring artifact.

## What it shows

- **The starter does the Temporal wiring.** No `@Configuration` for the client or
  Worker — `application.yml` + `@WorkflowImpl` / `@ActivityImpl` is all it takes.
  The only hand-wired beans are the AWS SDK clients (`AwsClientConfig`).
- **Two entry points, one Workflow.** The `SqsTriggerBridge` is the event-driven
  path (A drops a message → `signalWithStart`); `GlueController` is the
  synchronous REST path for demos/ops. Both start the same `GlueStitchWorkflow`.
- **`signalWithStart` for idempotency.** SQS is at-least-once; redeliveries that
  arrive **while a stitch is in flight** re-signal the one run (bumping a counter
  you can Query) instead of starting a concurrent duplicate. (Once a run has
  finished, a later trigger starts a fresh stitch — which is harmless, since the
  Glue job rewrites the same deterministic curated path.)
- **Supervised external compute.** `runGlueJob` starts the job and polls it to a
  terminal state, **heartbeating** each poll so a long Spark job that outlives a
  Worker restart resumes its poll instead of re-launching.
- **References, not bytes.** Activities pass S3 URIs; the SNS notification carries
  the curated URI + `workflowId` (for subscriber dedup), never the data.

## Run it

Three things up front (each in its own terminal as needed):

```bash
make temporal      # 1) Temporal dev server on :7233 / UI :8233
make stack-aws     # 2) LocalStack (S3/SQS/SNS) on :4566
make seed-glue     # 3) create bucket + SQS bus + SNS topic + A's inbox, land a Parquet partition
```

Then start the service:

```bash
make run-spring-glue
# or: scripts/run-example.sh spring-glue
```

It is a single process (the Spring app is the Worker, the SQS bridge, and the
REST client), listening on `:8080`. On boot you'll see the bridge start polling.

> **JDK note.** Spring Boot 3.3 targets Java 17–21. `run-example.sh` pins JDK 17
> if your default `java` is newer and a 17 is installed; it also runs fine on
> newer JDKs in practice (verified on JDK 26).

### Drive it — the event path (matches the diagram)

```bash
# A drops a "partition landed" message on the bus:
scripts/seed-glue-demo.sh trigger
# ... the bridge does signalWithStart; the Workflow runs validate → Glue → notify.

# Read A's inbox to see the SNS notification arrive:
scripts/seed-glue-demo.sh notify
# {"workflowId":"stitch-lake-raw-orders-dt-2026-06-17","runId":"…","status":"VALIDATED",
#  "fileCount":3,"totalBytes":…,"curatedS3Uri":"s3://lake/curated/orders/dt=2026-06-17/part-0000.parquet"}

# Confirm idempotency: a BURST of redeliveries while the stitch is in flight
# (the realistic at-least-once case) collapses into ONE run — triggerCount climbs,
# but the Web UI shows a single execution for the partition.
for i in 1 2 3; do scripts/seed-glue-demo.sh trigger; done
curl -s localhost:8080/stitch/stitch-lake-raw-orders-dt-2026-06-17 | jq .
# {"workflowId":"stitch-lake-raw-orders-dt-2026-06-17","status":"DONE","triggerCount":3}

# See the curated output the (faked) Glue job wrote:
scripts/seed-glue-demo.sh ls
```

### Drive it — the REST path (synchronous)

```bash
curl -s -X POST localhost:8080/stitch \
  -H 'content-type: application/json' \
  -d '{"bucket":"lake","prefix":"raw/orders/dt=2026-06-17/"}' | jq .
# {"fileCount":3,"totalBytes":…,"curatedS3Uri":"s3://lake/curated/…","notificationMessageId":"…"}
```

In the Web UI (`localhost:8233`) the execution is `stitch-…` on the
`glue-stitch` task queue; its history shows `validatePartition`,
`runGlueJob` (with heartbeats), and `publishValidation` in order.

## Running against real AWS

Nothing about the Workflow changes — only the edges:

1. **Clear `aws.endpoint`** (set it to empty / unset `AWS_ENDPOINT`). The clients
   then use the default endpoint resolver and the `DefaultCredentialsProvider`
   chain — the ECS task role or EKS IRSA — so there are no static keys.
2. **Use a real Glue job.** Add `software.amazon.awssdk:glue` and provide a
   `GlueJobRunner` backed by `GlueClient` (start `StartJobRun`, poll `GetJobRun`
   to `SUCCEEDED`, throw on `FAILED`/`TIMEOUT`/`STOPPED`). The `FakeGlueJobRunner`
   javadoc shows the exact shape; the real Glue Activity also lives in
   [`08-aws-containers`](../08-aws-containers/java/src/main/java/training/temporal/aws/GlueJobActivitiesImpl.java).
3. **Point at your topic/queue ARNs** in `application.yml` (or env vars).

## Where this sits

- The Spring Boot on-ramp this builds on: [`16-spring-boot`](../16-spring-boot/)
- The same Glue/S3/SNS/SQS pieces as standalone Day-6 labs:
  [`challenges/day-06-aws-containers`](../../../challenges/day-06-aws-containers/)
  (lab 1 Glue, lab 6 SQS trigger, lab 7 SNS notify)
- Containerizing the Worker: [`08-aws-containers`](../08-aws-containers/)
