# Day 6 — AWS migration & container workloads

Two halves. **Morning:** replace AWS Glue + Lambda + S3 + Step Functions
orchestration with Temporal, using LocalStack so no real AWS account is needed.
**Afternoon:** run the Worker as a container — Dockerfile, Kubernetes Deployment,
and KEDA autoscaling on Task Queue backlog — using a local `kind` cluster as an
EKS stand-in.

## Required stack

```bash
make temporal       # terminal 1: always

# Morning (AWS labs) — LocalStack mocks S3/SQS/Glue:
make stack-aws      # LocalStack on :4566

# Afternoon (container labs) — local Kubernetes + KEDA:
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

Labs 1–3 build up the `ImportWorkflow` (validate → transform → load → notify).
Labs 4–5 take that same Worker to containers and Kubernetes.

## Mapping AWS primitives to Temporal

| AWS | Temporal | Lab |
|---|---|---|
| Glue job (ETL) | Activity | 1 |
| S3 checkpoint between steps | Durable Workflow state | 2 |
| Step Functions state machine | Workflow | 3 |
| Lambda orchestrator / trigger | Workflow starter / Signal bridge | 3 |
| EventBridge rule → Lambda | SQS/Kafka consumer bridge → Signal | 5 |
| CloudWatch retry + DLQ | RetryOptions + compensation | 1–3 |
| ECS/EKS task | Worker container (Deployment) | 4–5 |
| HPA on CPU | KEDA on Task Queue backlog | 5 |

> Keep AWS compute where it earns its keep (terabyte Glue Spark joins); replace
> the **orchestration glue** (S3 checkpoints, Lambda chaining, Step Functions
> JSON).
