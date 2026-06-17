# Lab 6.9 — ECS Fargate + autoscale on Task Queue backlog

**Time:** ~45 min · **Difficulty:** ★★★ · **Stack:** real AWS (ECS Fargate) — optional

> **Optional · real AWS account required.** This lab provisions real ECS Fargate
> resources (small cost, ~cents/hr). It is **not** runnable on the free LocalStack
> tier — there are **no `make` targets**. Walk the manifests; apply them only if
> you have an account and want the full experience.

## Scenario

Lab 6.5 ran the Worker on Kubernetes and let **KEDA** autoscale it on Task Queue
backlog. This lab does the same thing on **ECS Fargate** — but ECS has **no native
Temporal scaler**. KEDA polls `DescribeTaskQueue` for you and feeds the result into
an HPA; on ECS you have to publish that signal yourself and let **Application Auto
Scaling** consume it. Same idea — *scale on queue depth, not CPU* — just more
wiring. You'll run the Worker as a Fargate **Service** (stateless, outbound-only,
no load balancer), publish the backlog as a **custom CloudWatch metric**, and
target-track the service's desired count on that metric.

> "Amazon ECS leverages the Application Auto Scaling service to provide this functionality."
>
> — *Amazon ECS Developer Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-auto-scaling.html -->

## Learning goals

- Run the Worker as an ECS Fargate **Service** with **no load balancer** — Workers
  take no inbound traffic, they dial *out* to the Frontend.
- Understand why ECS needs a **backlog publisher** where Kubernetes had KEDA, and
  wire **Application Auto Scaling** to target-track a custom CloudWatch metric.
- Split **execution role** (pull image, fetch SSM secrets, write logs — used by the
  ECS agent) from **task role** (what your code calls AWS with at runtime).
- Use `stopTimeout` as the ECS analog of `terminationGracePeriodSeconds`: drain
  in-flight Activities on `SIGTERM` instead of killing them.

## What you'll need

This lab has **no `make` targets**. To actually apply it you need:

- An AWS account and the `aws` CLI configured (a region, e.g. `us-east-1`).
- A reachable Temporal Frontend — **Temporal Cloud** is easiest (an API key in
  SSM); a self-hosted Frontend works if your Fargate subnets can route to it.
- An ECS cluster (`aws ecs create-cluster --cluster-name temporal-training`), a VPC
  with **private** subnets + a NAT (or VPC endpoints) so tasks reach ECR/SSM/
  CloudWatch and your Frontend, and an ECR repo for the Worker image.
- The Worker image from **Lab 6.4** (the same `Dockerfile`) pushed to that ECR repo.

If you don't have an account, **read the manifests and the "Coming from" mapping** —
the concepts transfer directly from Lab 6.5.

## The manifests

All four live under
[`examples/07-aws-containers/aws/`](../../examples/07-aws-containers/aws). JSON has
no comments, so the explanation lives here.

**[`ecs_task_definition.json`](../../examples/07-aws-containers/aws/ecs_task_definition.json)**
— the Fargate task: the Worker container from the Lab 6.4 `Dockerfile`, `awsvpc`
networking, and the two roles kept distinct:

- `executionRoleArn` — the **ECS agent's** identity: pull the image from ECR,
  resolve the `secrets` from SSM, push logs to CloudWatch. Needs
  `AmazonECSTaskExecutionRolePolicy` + `ssm:GetParameters` on your parameter ARNs.
- `taskRoleArn` — **your code's** identity at runtime (S3, the backlog publisher's
  `cloudwatch:PutMetricData`). This is the role your Activities assume — the
  ECS/IRSA analog of Lab 6.5's `serviceAccountName`.

`TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TASK_QUEUE` are plain `environment`;
the API key and DB password come from `secrets` with `valueFrom` SSM ARNs (forward
ref: **Lab 6.8** covers SSM/Secrets Manager config in depth). `stopTimeout: 120`
gives in-flight Activities time to drain on `SIGTERM` — set it ≥ your longest
`startToCloseTimeout`, exactly like `terminationGracePeriodSeconds` in Lab 6.5.

**[`ecs_service.json`](../../examples/07-aws-containers/aws/ecs_service.json)** —
the Service that keeps `desiredCount` tasks running. Note **`loadBalancers: []`**:
Workers are outbound-only, so there is no load balancer, no target group, no health
check on an inbound port — the direct mirror of "Workers are just Deployments
(never Services)" from Lab 6.5. `networkConfiguration` places tasks on **private**
subnets with `assignPublicIp: DISABLED`; egress to ECR/SSM/CloudWatch/Frontend goes
via NAT or VPC endpoints.

**[`ecs_autoscaling.json`](../../examples/07-aws-containers/aws/ecs_autoscaling.json)**
— two calls. `registerScalableTarget` registers
`ecs:service:DesiredCount` with `MinCapacity: 1` / `MaxCapacity: 10` (same envelope
as KEDA's `minReplicaCount` / `maxReplicaCount`). `putScalingPolicy` is a
**TargetTrackingScaling** policy whose `CustomizedMetricSpecification` points at the
custom metric `Temporal/Worker` / `TaskQueueBacklog` (dimension
`TaskQueue=transform`) with `TargetValue: 20` — Application Auto Scaling adds/removes
tasks to hold ~20 backlog items per task, the exact role of KEDA's
`targetQueueSize: "20"`.

**[`backlog_publisher.md`](../../examples/07-aws-containers/aws/backlog_publisher.md)**
— the piece KEDA gave you for free. A ~30-line loop: call `DescribeTaskQueue`, sum
the backlog, `PutMetricData` into `Temporal/Worker` / `TaskQueueBacklog`. Run it as
a sidecar in the task or as one scheduled task for the whole queue. **Without this
publisher there is no metric, and the scaling policy never fires.**

## Coming from Kubernetes + KEDA (Lab 6.5)

> | Lab 6.5 (KEDA) | This lab (ECS) |
> |---|---|
> | Worker `Deployment` | ECS Fargate **Service** (`loadBalancers: []`) |
> | `replicas` / HPA `desiredReplicas` | Service `desiredCount` |
> | `terminationGracePeriodSeconds: 120` | container `stopTimeout: 120` |
> | `serviceAccountName` (IRSA) | **task role** |
> | image pull + secrets plumbing (kubelet) | **execution role** |
> | KEDA `temporal` trigger polls `DescribeTaskQueue` | **you** poll it in a backlog publisher |
> | KEDA emits the metric → HPA | publisher → **CloudWatch custom metric** |
> | `targetQueueSize: "20"` | Application Auto Scaling `TargetValue: 20` |
> | `min/maxReplicaCount` | `register-scalable-target` Min/MaxCapacity |
>
> KEDA bundles the poller, the metric, and the scaler into one `ScaledObject`. On
> ECS those are three separate things you assemble: the publisher (poller +
> metric), CloudWatch (storage), and Application Auto Scaling (the scaler). The
> *decision* — scale on queue depth — is identical; ECS just makes you build the
> pipe that carries the signal.

## Tasks

1. **Push the image.** Build the Lab 6.4 image, tag it for your ECR repo, and push:
   `aws ecr get-login-password | docker login ...`, then `docker push`.
2. **Register the task definition.**
   `aws ecs register-task-definition --cli-input-json file://examples/07-aws-containers/aws/ecs_task_definition.json`
   (fill in your `<account-id>`, ECR/SSM ARNs, and `TEMPORAL_*` values first).
3. **Create the service.**
   `aws ecs create-service --cli-input-json file://examples/07-aws-containers/aws/ecs_service.json`
   (your cluster + private subnets + an egress-only security group). Confirm tasks
   reach `RUNNING` and the logs show `Worker started ... taskQueue=transform`.
4. **Wire autoscaling.** Run the two calls in `ecs_autoscaling.json`:
   `aws application-autoscaling register-scalable-target ...` then
   `... put-scaling-policy ...`.
5. **Run the backlog publisher** (sidecar or a one-off task) so
   `Temporal/Worker`/`TaskQueueBacklog` starts appearing in CloudWatch.
6. **Drive load and watch it scale.** Flood the `transform` queue (start ~200
   Workflows) and watch `desiredCount` / `runningCount` climb toward `MaxCapacity`,
   then settle back to `MinCapacity` as the backlog drains.

## Verification

```bash
# Tasks running and climbing as backlog grows:
aws ecs describe-services \
  --cluster temporal-training --services temporal-transform-worker \
  --query 'services[0].{desired:desiredCount,running:runningCount,pending:pendingCount}'

# The custom metric the publisher is writing (this is your KEDA replacement):
aws cloudwatch get-metric-statistics \
  --namespace Temporal/Worker --metric-name TaskQueueBacklog \
  --dimensions Name=TaskQueue,Value=transform \
  --start-time "$(date -u -v-15M +%FT%TZ)" --end-time "$(date -u +%FT%TZ)" \
  --period 60 --statistics Average

# Drive backlog (any Workflow starter against the transform queue):
for i in $(seq 1 200); do
  temporal workflow start --task-queue transform --type ImportWorkflow \
    --workflow-id importworkflow-$i --input "\"s3://imports-incoming/synthetic-$i.csv\""
done
```

Expected: as backlog passes ~20 items/task, `runningCount` rises toward
`MaxCapacity: 10`; once drained it settles back to `MinCapacity: 1`. This is the
ECS counterpart of `kubectl get scaledobject` + `kubectl get hpa -w` from Lab 6.5 —
the difference is that *you* can see the raw metric in CloudWatch because you
published it, whereas KEDA hid it inside the `ScaledObject`.

## Definition of done

- [ ] Worker runs as a Fargate **Service** with **no load balancer** (outbound-only).
- [ ] Task definition uses a distinct **execution role** (pull/secrets/logs) and
      **task role** (runtime AWS calls), and `stopTimeout` ≥ your longest
      `startToCloseTimeout`.
- [ ] A backlog publisher writes `Temporal/Worker`/`TaskQueueBacklog` to CloudWatch.
- [ ] Application Auto Scaling target-tracks that metric (`TargetValue: 20`) —
      **not** CPU — between Min 1 and Max 10.
- [ ] Backlog growth raises `runningCount`; drain returns it to `MinCapacity`.

## Pitfalls

- **Don't attach a load balancer.** Workers take no inbound traffic; an ALB/target
  group with health checks will mark a perfectly healthy outbound-only task
  unhealthy and churn it. `loadBalancers: []` is correct, not an omission.
- **Execution role vs task role mixups** are the #1 ECS gotcha. Missing
  `ssm:GetParameters` / ECR pull on the **execution** role → tasks won't start
  (the agent fails before your code runs). Missing `cloudwatch:PutMetricData` on
  the **task** role → tasks run but the publisher silently fails and nothing scales.
- **CPU-based scaling is the wrong signal** (same as Lab 6.5): a poller can sit idle
  on CPU while a deep backlog waits. Target-track the backlog metric, not
  `ECSServiceAverageCPUUtilization`.
- **No publisher, no scaling.** Application Auto Scaling treats a missing metric as
  "no breach" — it just sits there. Confirm the metric exists in CloudWatch before
  blaming the policy.
- **Remember to scale down / delete.** Fargate bills per running task. A service
  stuck at `MaxCapacity` because the publisher died keeps charging.

## Cost

Fargate charges per vCPU-second and GB-second **per running task**, so a service at
`desiredCount: 10` costs ~10× one at `1`. When you're done:

```bash
aws application-autoscaling deregister-scalable-target \
  --service-namespace ecs --scalable-dimension ecs:service:DesiredCount \
  --resource-id service/temporal-training/temporal-transform-worker
aws ecs update-service --cluster temporal-training \
  --service temporal-transform-worker --desired-count 0
aws ecs delete-service --cluster temporal-training \
  --service temporal-transform-worker --force
```

Also delete the CloudWatch log group and any standalone publisher task. NAT
gateways bill hourly even when idle — tear down the VPC scaffolding if you created
it just for this lab.

## Hints

<details><summary>Hint 1 — tasks stuck in PENDING / failing to start</summary>

Almost always the **execution role** or networking. Check `aws ecs
describe-tasks --tasks <id>` `stoppedReason`: `CannotPullContainerError` → ECR pull
perms / no route to ECR; `ResourceInitializationError` fetching SSM → missing
`ssm:GetParameters` on the execution role or no SSM VPC endpoint/NAT. Private
subnets need a NAT or VPC endpoints for ECR, SSM, CloudWatch Logs.
</details>

<details><summary>Hint 2 — metric written but nothing scales</summary>

Confirm the namespace, metric name, and **dimensions** in your publisher match
`ecs_autoscaling.json` *exactly* (`Temporal/Worker` / `TaskQueueBacklog` /
`TaskQueue=transform`). A dimension mismatch means the policy reads a different
(empty) metric and never breaches. Then check the policy is attached:
`aws application-autoscaling describe-scaling-policies --service-namespace ecs`.
</details>

## Stretch goals

- **Scale to zero with a scheduled wake.** Set `MinCapacity: 0` for a pure Activity
  pool and add a scheduled-action (or an `activationTargetQueueSize`-style floor in
  the publisher) that bumps capacity to 1 when backlog first appears — Application
  Auto Scaling won't scale *up from* 0 on a target-tracking metric alone. (Keep
  Workflow workers ≥ 1, same caveat as Lab 6.5.)
- **Publish schedule-to-start latency instead of raw backlog.** Emit the queue's
  schedule-to-start P95 (how long tasks wait before a poller picks them up) as the
  metric and target-track on *that*. Latency is the SLO users feel and is robust to
  bursty-but-fast queues where raw depth over-reacts.
