# Day 6: AWS migration & container workloads

Two halves. **Morning:** replace AWS Glue + Lambda + S3 + Step Functions
orchestration with Temporal, using LocalStack so no real AWS account is needed.
(Glue itself is a paid-tier LocalStack emulator, so lab 1 supervises a self-hosted
local job instead of calling it — and the pinned LocalStack image runs token-free;
see [Setup.md](../../Setup.md) for the 2026 free-tier change.)
**Afternoon:** run the Worker as a container (Dockerfile, Kubernetes Deployment,
and KEDA autoscaling on Task Queue backlog) using a local `kind` cluster as an
EKS stand-in.

Beyond the core five, two extra tiers go deeper into the AWS surface:

- **Extended LocalStack labs (6–8)**: still free and local, an SQS *event
  trigger*, SNS *fan-out notifications*, and worker config/secrets from SSM
  **Parameter Store**. These round out the morning's "what drives a Workflow,
  and how does it talk back out".
- **Optional real-AWS labs (9–12)**: ECS Fargate, EKS + IRSA, Aurora, and
  Route 53. LocalStack's free tier can't mock these, so each is **conceptual +
  reference manifests** with no `make` targets. Read the manifests and the
  reasoning; apply them only if you have an account (they cost real money, so
  tear down after). Each is flagged `★ optional · real AWS` at the top.

## Required stack

```bash
make temporal       # terminal 1: always

# Morning (AWS labs): LocalStack provides S3/SQS/SNS/SSM (Glue is paid-tier — see lab 1):
make stack-aws      # LocalStack on :4566

# Afternoon (container labs): local Kubernetes + KEDA:
make kind-up        # create kind cluster + install KEDA via Helm
make kind-load      # build the Worker image and load it into kind

# One-shot for the whole day:
make day-6-up       # stack-aws + kind-up + kind-load
make day-6-down     # tear it all down
```

`awslocal` (the LocalStack-aware `aws` CLI), `kubectl`, `helm`, and `kind` come
from the full setup (`make setup-mac-full` / `make setup-ubuntu-full`).

## Labs

| # | Lab | Time | Difficulty | Stack |
|---|-----|------|-----------|-------|
| 1 | [Wrap a Glue job as an Activity](lab-1-glue-activity.md) | 50 min | ★★ | `stack-aws` |
| 2 | [Replace S3 checkpointing](lab-2-s3-checkpointing.md) | 40 min | ★★ | `stack-aws` |
| 3 | [Migrate a Step Functions pipeline](lab-3-stepfunctions-migration.md) | 60 min | ★★★ | `stack-aws` |
| 4 | [Containerize the Worker](lab-4-worker-container.md) | 45 min | ★★ | Docker |
| 5 | [Kubernetes + KEDA autoscaling](lab-5-kubernetes-keda.md) | 70 min | ★★★ | `kind` + KEDA |

**Extended LocalStack labs**: still free, still local (`make stack-aws`; `make aws-init` seeds the queue, topic, and params):

| # | Lab | Time | Difficulty | Stack |
|---|-----|------|-----------|-------|
| 6 | [SQS event trigger → `signalWithStart`](lab-6-sqs-event-trigger.md) | 45 min | ★★ | `stack-aws` |
| 7 | [SNS fan-out notify Activity](lab-7-sns-fanout-notify.md) | 40 min | ★★ | `stack-aws` |
| 8 | [Worker config & secrets from SSM Parameter Store](lab-8-ssm-parameter-store.md) | 40 min | ★★ | `stack-aws` |

**Optional real-AWS labs**: conceptual + reference manifests only, **no `make` targets**, costs real money (tear down after):

| # | Lab | Time | Difficulty | Stack |
|---|-----|------|-----------|-------|
| 9 | [ECS Fargate + autoscale on backlog](lab-9-ecs-fargate-autoscaling.md) | 45 min | ★★★ | real AWS |
| 10 | [Graduate the Worker to EKS with IRSA](lab-10-eks-irsa-deployment.md) | 70 min | ★★★ | real AWS |
| 11 | [Aurora as the transactional sink](lab-11-aurora-transactional-sink.md) | 50 min | ★★★ | real AWS |
| 12 | [Route 53 resilient frontend endpoint](lab-12-route53-failover.md) | 40 min | ★★★ | real AWS |

Labs 1–3 build up the `ImportWorkflow` (validate → transform → load → notify).
Labs 4–5 take that same Worker to containers and Kubernetes. Labs 6–8 wire the
events around it (SQS in, SNS out, SSM for config). Labs 9–12 are the production
AWS shapes you'd run it on.

## Mapping AWS primitives to Temporal

| AWS | Temporal | Lab |
|---|---|---|
| Glue job (ETL) | Activity | 1 |
| S3 checkpoint between steps | Durable Workflow state | 2 |
| S3 as a blob store | Activity passes a URI, not bytes | 2 |
| Step Functions state machine | Workflow | 3 |
| Lambda orchestrator / trigger | Workflow starter / Signal bridge | 3 |
| EventBridge rule → Lambda → StartExecution | SQS consumer bridge → `signalWithStart` | 6 |
| SNS publish in a Lambda step | An Activity publishes; subscribers stay decoupled | 7 |
| SSM Parameter Store / Secrets Manager | Config read at Worker startup; secrets via an Activity | 8 |
| CloudWatch retry + DLQ | RetryOptions + compensation | 1–3 |
| ECS service / EKS Deployment | Worker container (stateless, outbound-only) | 4–5, 9–10 |
| HPA on CPU | KEDA on Task Queue backlog (EKS) / Application Auto Scaling on a published backlog metric (ECS) | 5, 9 |
| IAM access keys in the image | IRSA (EKS) / task role (ECS), no static keys | 8–10 |
| DynamoDB / Aurora idempotency table | At-least-once Activity + conditional/`ON CONFLICT` write = effectively-once | 11 |
| ALB/NLB hostname hardcoded in config | Stable Route 53 name + health-checked failover | 12 |

> Keep AWS compute where it earns its keep (terabyte Glue Spark joins); replace
> the **orchestration glue** (S3 checkpoints, Lambda chaining, Step Functions
> JSON).
