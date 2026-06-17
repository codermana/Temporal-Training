# Lab 6.10 — Graduate the Worker to EKS with IRSA

> **Optional · real AWS account required.** Provisions a real EKS cluster
> (~$0.10/hr control plane + node costs). **Not** runnable on free LocalStack —
> there are **no `make` targets**. The kind cluster from Lab 6.5 is the local
> stand-in; this lab is the production graduation. Walk the manifests; apply
> only with an account.

**Time:** ~70 min · **Difficulty:** ★★★ · **Stack:** real AWS (EKS) — optional

## Scenario

In Lab 6.5 you ran the Worker Deployment and a KEDA `ScaledObject` on a local
`kind` cluster. Now you take **the exact same workload** to a real EKS cluster.
The surprise is how little changes: the Deployment, the probes, the grace
period, and the KEDA `ScaledObject` are unchanged. That sameness is Temporal's
portability story — a Worker is a stateless, outbound-only process, so the
cluster underneath it is an implementation detail.

The only genuinely new additions are operational, not Temporal: a **real
cluster** (eksctl), and **IRSA** so the Activities reach AWS (S3/Glue/SNS/SSM)
with **no static access keys** — the production answer to the LocalStack
`test`/`test` dummy creds you used all morning. As the slide note puts it:
**"IRSA, not access keys."**

> "Instead of creating and distributing your AWS credentials to the containers or using the Amazon EC2 instance's role, you associate an IAM role with a Kubernetes service account and configure your Pods to use the service account."
>
> — *Amazon EKS User Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html -->

## Learning goals

- See that a Temporal Worker is **portable**: kind → EKS is a no-op for the
  Deployment and the KEDA `ScaledObject`.
- Replace static AWS access keys with **IRSA** — a ServiceAccount mapped to an
  IAM role via the cluster's OIDC provider; Activities get temporary creds.
- Run the **same KEDA `ScaledObject`** that scaled on kind; on EKS it scales
  pods on **real infrastructure**.
- Understand that **Cluster Autoscaler / Karpenter** adds nodes when KEDA-driven
  pods go `Pending` — scaling pods can cascade into scaling the cluster.

## What you'll need

- A real **AWS account** (this provisions billable resources — see **Cost**).
- **`eksctl`**, **`kubectl`**, and **`helm`** installed and authenticated.
- An **ECR repository** for the Worker image, e.g.
  `aws ecr create-repository --repository-name temporal-transform-worker`.
- The Worker **image built and pushed** to that ECR repo (the Lab 6.4
  Dockerfile, retagged to the ECR URI and `docker push`ed after
  `aws ecr get-login-password | docker login`).
- A reachable **Temporal Frontend**: either **Temporal Cloud** (set
  `TEMPORAL_ADDRESS`, the namespace, and mTLS/API-key env — the usual Cloud
  connection) or a **self-hosted** frontend behind an internal ELB. See the
  "Temporal Cloud vs EKS self-hosted" tradeoff in the slides.

## The manifests

Three reference files under
[`examples/07-aws-containers/aws/`](../../examples/07-aws-containers/aws/). The
KEDA `ScaledObject` you reuse is the **same**
[`keda_scaledobject.yaml`](../../examples/07-aws-containers/keda_scaledobject.yaml)
from Lab 6.5 — there is no EKS-specific copy, and that is the point.

**`eksctl-cluster.yaml`** — an eksctl `ClusterConfig` in `us-east-1` with one
managed nodegroup and, crucially, `iam.withOIDC: true`. That single line
provisions the IAM OIDC identity provider that IRSA depends on. (A
`terraform-aws-modules/eks` config with `enable_irsa = true` is the equivalent
if you prefer Terraform.)

**`irsa-serviceaccount.yaml`** — a ServiceAccount named `temporal-worker`
annotated with `eks.amazonaws.com/role-arn` pointing at
`arn:aws:iam::<account-id>:role/temporal-worker-irsa`. The file's comment block
spells out the **trust policy** (the OIDC `sub` condition that maps *only* this
ServiceAccount to the role) and the **least-privilege permissions** the Worker
needs: `s3` (get/put/list), `glue` (StartJobRun/GetJobRun), `sns:Publish`, and
`ssm:GetParameter*`.

**`eks-worker-deployment.yaml`** — the Lab 6.5 Worker Deployment with exactly
**two diffs**, both flagged `# EKS DIFF` in the file: `serviceAccountName:
temporal-worker` (binds the pod to the IRSA role) and an `image:` that points at
your **ECR** repo instead of a kind-loaded tag. The exec readiness/liveness
probes, `maxUnavailable: 0`, and `terminationGracePeriodSeconds: 120` are
identical to kind.

## Coming from [containers] / [aws]

- **From Lab 6.5 (kind):** moving the Worker from kind to EKS is a **no-op** for
  the Deployment and the KEDA `ScaledObject`. You don't rewrite the workload;
  you give it a real cluster and a real identity. If you find yourself editing
  the Worker spec, you're probably doing too much.
- **From the morning AWS labs (LocalStack):** the `test`/`test` static
  credentials and `endpointOverride(http://127.0.0.1:4566)` were a local
  stand-in. In production you delete the static keys entirely — **IRSA** hands
  the AWS SDK temporary, auto-rotated credentials through the default provider
  chain. No keys in the image, no `Secret` to leak.

## Tasks

1. **Create the cluster:** `eksctl create cluster -f
   examples/07-aws-containers/aws/eksctl-cluster.yaml`. Confirm OIDC is enabled
   (eksctl reports the OIDC provider URL).
2. **Create the IAM role + IRSA trust.** Easiest:
   `eksctl create iamserviceaccount --name temporal-worker --namespace default
   --cluster temporal-training --attach-policy-arn <your-worker-policy>
   --approve` (this writes the trust policy condition for you). Or create the
   role and trust policy by hand per the comment block in
   `irsa-serviceaccount.yaml`, then `kubectl apply -f irsa-serviceaccount.yaml`.
3. **Install KEDA** via Helm — the **same chart** as kind:
   `helm repo add kedacore https://kedacore.github.io/charts && helm repo update`
   then `helm install keda kedacore/keda --namespace keda --create-namespace
   --wait`.
4. **Apply the workload.** Create the `temporal-worker-config` ConfigMap
   (pointing `temporal-address` at your Cloud or self-hosted frontend), then
   `kubectl apply -f examples/07-aws-containers/aws/eks-worker-deployment.yaml`
   and `kubectl apply -f examples/07-aws-containers/keda_scaledobject.yaml` —
   the **same** ScaledObject from Lab 6.5.
5. **Drive load and watch it scale.** Flood the `transform` Task Queue, then
   watch **both** pods and **nodes** scale: `kubectl get pods -w` and, in
   another pane, `kubectl get nodes -w`. With Cluster Autoscaler / Karpenter
   installed, pods that go `Pending` for lack of room trigger new nodes.

## Verification

```bash
# Pods scale on Task Queue backlog, just like Lab 6.5 — but on a real cluster:
kubectl get scaledobject,deployment,pods
kubectl get hpa -w        # KEDA manages an HPA under the hood
kubectl get pods -w

# Prove IRSA works — no static keys anywhere. Exec into a running Worker pod
# and ask AWS who you are; the answer is the ASSUMED IRSA role, not a user:
POD=$(kubectl get pod -l app=temporal-transform-worker -o name | head -1)
kubectl exec "$POD" -- aws sts get-caller-identity
# Expect an ARN like:
#   arn:aws:sts::111122223333:assumed-role/temporal-worker-irsa/botocore-session-...

# Under sustained backlog, the cluster itself grows:
kubectl get nodes -w
```

Expected: replica count climbs past 2 toward `maxReplicaCount` while backlog is
high (identical behaviour to Lab 6.5), then settles back to `minReplicaCount`
once drained. `aws sts get-caller-identity` from inside the pod returns the
**assumed-role** ARN — proof the Activities authenticate via IRSA with no
embedded keys. Node count grows when pods go `Pending`.

## Definition of done

- [ ] An EKS cluster with `iam.withOIDC: true` is up and `kubectl` reaches it.
- [ ] The `temporal-worker` ServiceAccount is annotated with the IRSA role ARN
      and the role's trust policy is conditioned on the OIDC `sub`.
- [ ] The **unchanged** Lab 6.5 Deployment (plus the two EKS diffs) and the
      **same** KEDA `ScaledObject` are applied and the Worker polls `transform`.
- [ ] `aws sts get-caller-identity` inside the pod shows the **assumed IRSA
      role** — no static access keys present.
- [ ] Backlog growth scales pods; sustained backlog grows the **node** count.
- [ ] You ran `eksctl delete cluster` afterward to stop paying.

## Pitfalls

- **Don't bake access keys into the image.** Mounting `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` defeats the entire exercise — IRSA exists precisely so
  you never do this. If `get-caller-identity` shows a *user* (not an
  assumed-role), static keys are leaking in from somewhere; remove them.
- **OIDC provider must be enabled** or IRSA fails **silently** — the
  `eks.amazonaws.com/role-arn` annotation is ignored and the SDK falls back to
  node-role creds (or none). Confirm `iam.withOIDC: true` took effect.
- **Workers are outbound-only** — there is still **no `Service` and no
  `Ingress`**. The Worker dials *out* to the Frontend and to AWS APIs; nothing
  dials in. Don't add one because "it's on EKS now."
- **Remember to `eksctl delete cluster`.** The control plane bills per hour and
  the nodes bill continuously whether or not work is flowing. Tear it down.

## Cost

A rough order of magnitude: the EKS control plane is **~$0.10/hr** (~$73/mo);
two `m5.large` nodes add a few dollars a day; a NAT gateway and any data egress
add more. Scaling up under load multiplies node cost. This is a **bring-your-
own-account, optional** lab — if you only want the concepts, walk the manifests
and skip the apply. When you do run it, **`eksctl delete cluster -f
examples/07-aws-containers/aws/eksctl-cluster.yaml`** when finished.

## Hints

<details><summary>Hint 1 — the IRSA trust policy condition</summary>

The trust policy on `role/temporal-worker-irsa` is what scopes the role to a
single ServiceAccount. It allows `sts:AssumeRoleWithWebIdentity` from the
cluster's OIDC provider **only when** the token's `sub` claim equals
`system:serviceaccount:default:temporal-worker` (and `aud` is
`sts.amazonaws.com`). Get the namespace or SA name wrong in that condition and
the pod can't assume the role even though the annotation looks correct. The full
JSON is in the comment block of `irsa-serviceaccount.yaml`;
`eksctl create iamserviceaccount` generates it for you.
</details>

<details><summary>Hint 2 — debugging IRSA</summary>

`kubectl exec <pod> -- aws sts get-caller-identity` is the fastest probe. If it
returns the **assumed-role** ARN, IRSA works. If it errors or shows a node role,
check, in order: `iam.withOIDC: true` actually provisioned an OIDC provider
(`aws iam list-open-id-connect-providers`); the pod's ServiceAccount is
`temporal-worker` (not `default`); the annotation ARN is right; and the role's
trust `sub` matches `system:serviceaccount:<ns>:temporal-worker`. Also confirm
EKS injected `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` into the pod env.
</details>

## Stretch goals

- Replace the managed nodegroup with **Karpenter**: let it provision
  right-sized nodes just-in-time as KEDA-created pods go `Pending`, instead of a
  fixed-size group. Compare scale-up latency and bin-packing efficiency.
- Set the KEDA `ScaledObject` `minReplicaCount: 0` to **scale the Activity
  worker pool to zero** on EKS when the `transform` queue is empty, and watch it
  cold-start a pod (and possibly a node) on the next backlog. Keep **Workflow**
  workers at `≥ 1` so timers and sticky execution keep making progress.
