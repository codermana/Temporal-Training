# Lab 6.12 — Route 53 for a resilient Temporal Frontend endpoint

> **Optional · real AWS account required.** Uses a real Route 53 hosted zone +
> health checks (a hosted zone is ~$0.50/mo; health checks ~cents). **Not**
> runnable on free LocalStack — there are **no `make` targets**. Walk the records
> and the failover model; apply only with an account and a domain.

**Time:** ~40 min · **Difficulty:** ★★★ · **Stack:** real AWS (Route 53) — optional

> **On Temporal Cloud?** You can skip this — Cloud gives you a managed global
> endpoint (`<ns>.<acct>.tmprl.cloud:7233`). This lab is for **self-hosters** on
> EKS/ECS who run their own Frontend and want a stable, resilient address for it.

## Scenario

Your Workers and clients need to dial a self-hosted Temporal Frontend. Today they
point at a region-specific load-balancer hostname — and that hostname changes if
the LB is recreated, and points at exactly one region. You'll put the Frontend
behind **one stable DNS name** (`temporal.internal.example.com:7233`) using
Route 53, then add **health-checked failover** so that if the primary region's
Frontend goes unhealthy, the name resolves to a secondary region instead. Along
the way you'll meet the single most important caveat in this whole area: **DNS
failover makes a Frontend *reachable*; it does not make your Workflow *data* be
there.**

> "Failover routing lets you route traffic to a resource when the resource is healthy or to a different resource when the first resource is unhealthy."
>
> — *Amazon Route 53 Developer Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-failover.html -->

## Learning goals

- A **stable DNS endpoint** decouples clients from the LB hostname — `TEMPORAL_ADDRESS`
  never changes even when the load balancer is recreated.
- **Health-checked FAILOVER** routing (PRIMARY/SECONDARY) for frontend
  reachability across regions.
- The crucial distinction: **DNS failover (reachability) vs. Temporal
  multi-cluster replication (data durability)** — DNS failover ≠ data replication.
- **Private hosted zones** so Workers in private subnets resolve the internal name.
- The **gRPC connection-caching** caveat — long-lived connections don't honor DNS
  changes instantly; failover helps reconnects, not live connections.

## What you'll need

- A **real AWS account** (Route 53 hosted zones + health checks aren't free, and
  LocalStack can't simulate them meaningfully).
- A **domain / hosted zone** you control (e.g. `internal.example.com`).
- **One or two regional Temporal Frontends behind NLBs** — a self-hosted Temporal
  cluster per region with its Frontend service fronted by an internal Network Load
  Balancer. For standing up the cluster on Kubernetes, reference the EKS lab
  (Lab 6.5 / `eksctl-cluster.yaml`); this lab assumes you already have at least a
  primary Frontend NLB and, ideally, a secondary in another region.

## The records

Two reference files under [`examples/07-aws-containers/aws/`](../../examples/07-aws-containers/aws/):

- **[`route53_failover.tf`](../../examples/07-aws-containers/aws/route53_failover.tf)** —
  Terraform for the whole thing: a **private hosted zone** associated with the
  Worker VPC(s); an `aws_route53_health_check` for the **primary** and the
  **secondary** Frontend; two `aws_route53_record` entries sharing the same name,
  distinguished by `set_identifier`, with `failover_routing_policy` PRIMARY /
  SECONDARY, each `alias`ed to its regional NLB and wired to its health check.
  Every block is commented to teach what it does — including the
  DNS-failover-≠-replication caveat, spelled out twice because it's the trap.
- **[`route53_records.md`](../../examples/07-aws-containers/aws/route53_records.md)** —
  the record-layout table, a text diagram of *Workers →
  `temporal.internal.example.com` → [primary NLB region A | secondary NLB region B]*,
  the gRPC connection-caching caveat, and an explicit "what DNS failover does and
  does **not** give you" box.

Read both before touching the console — the model matters more than the clicks.

## Coming from AWS

If you've run services behind AWS load balancers, the instinct is to **hardcode
the ALB/NLB hostname** (`...elb.us-east-1.amazonaws.com`) into your client config.
That couples every Worker to a specific LB in a specific region: recreate the LB
and the hostname changes; lose the region and there's nowhere else to go. The
move here is the same one you'd make for any service that must outlive its
infrastructure — **a stable Route 53 name with health-checked failover** in front
of the LB(s), so config holds a name, not an address.

## Tasks

1. **Create the private hosted zone** (`internal.example.com`) and associate it
   with the VPC(s) where your Workers/clients run, so in-VPC queries resolve the
   internal name to the internal NLB.
2. **Create health checks** — one per regional Frontend. Make each one reflect
   *Frontend* health (its gRPC health service / HTTP health sidecar), not just
   "the TCP port is open".
3. **Create the PRIMARY/SECONDARY failover records** for
   `temporal.internal.example.com`, both aliased to their regional NLBs, each
   bound to its health check via `health_check_id`, distinguished by
   `set_identifier`.
4. **Point `TEMPORAL_ADDRESS`** at the stable name
   (`temporal.internal.example.com:7233`) in your Worker Deployment / client
   config — drop the hardcoded LB hostname.
5. **Simulate a primary failure** (disable / fail the primary health check) and
   watch resolution flip to the secondary; restore it and watch it flip back.

## Verification

```bash
# 1. With the primary healthy, the name resolves to the PRIMARY NLB:
dig +short temporal.internal.example.com
# -> addresses of the us-east-1 (primary) Frontend NLB

# 2. Mark the primary unhealthy (Task 5) and re-query AFTER the TTL / health
#    failure threshold elapses:
dig +short temporal.internal.example.com
# -> now the us-west-2 (secondary) NLB addresses

# 3. Confirm a Worker reconnects to the secondary Frontend:
#    a freshly (re)started Worker re-resolves the name and connects to secondary.
#    (A long-running Worker only switches when its gRPC connection drops/reconnects
#    — see the gRPC caveat. Restart it to force a re-resolve.)
```

Expected: with the primary healthy, `dig` returns the primary NLB. After the
primary's health check fails (and the TTL/threshold elapses), `dig` returns the
secondary NLB, and a (re)connecting Worker reaches the secondary Frontend.

## Definition of done

- [ ] A **private hosted zone** resolves `temporal.internal.example.com` from
      inside the Worker VPC.
- [ ] **PRIMARY/SECONDARY failover records** exist, each aliased to its regional
      NLB and bound to a health check.
- [ ] `TEMPORAL_ADDRESS` holds the **stable name**, not an LB hostname.
- [ ] Failing the primary health check flips resolution to the secondary; a
      (re)connecting Worker reaches it.
- [ ] You can state out loud why this is **reachability, not durability** — and
      what you'd add (Temporal multi-cluster replication) for real DR.

## Pitfalls

- **THE BIG ONE — DNS failover does NOT replicate Workflow state.** When the name
  flips to the secondary region, that region's Frontend talks to *its own*
  Temporal cluster, which has **none of your Workflow history** unless you run
  **Temporal multi-cluster replication**. DNS failover = "clients reach a
  Frontend"; replication = "the data is actually there". They are different
  problems; this lab solves only the first. Don't ship a "DR plan" that's just
  DNS.
- **Long-lived gRPC connections don't instantly honor DNS changes.** The SDK
  resolves once at connect time and holds connections to those IPs. Failover
  affects **new connections / reconnects**, not live healthy ones — set sane TTLs
  and expect cutover on reconnect, not mid-call. For a single dead Frontend
  *instance*, **NLB** health drops it in seconds, faster than DNS ever will.
- **Public vs. private hosted zone mismatch.** If the private zone isn't
  associated with the Worker VPC, queries fall through to the public zone (or
  fail), resolving to the wrong place. In-VPC resolution must hit the private
  zone for the *internal* NLB.
- **Health-check endpoint must reflect Frontend health, not just TCP-open.** A
  plain TCP check on `7233` goes green the moment the NLB has any target — even
  one whose cluster is degraded. Probe the actual Frontend health service so
  "healthy" means "can serve Workflows". (For a fully private Frontend,
  Route 53's global checkers can't reach it — use a CloudWatch-alarm-backed
  health check instead.)

## Cost

A hosted zone is ~**$0.50/mo**; each health check is a **few cents/mo** (slightly
more for HTTPS/string-match or higher-frequency checks). There's no free-tier or
LocalStack path — these are real, billed Route 53 resources. Cheap, but delete
the zone/records/health checks when you're done with the lab if you don't intend
to keep them.

## Hints

<details><summary>Hint 1 — wiring a failover record to its health check</summary>

A failover record needs three things to behave: a `failover_routing_policy` block
(`type = "PRIMARY"` or `"SECONDARY"`), a unique `set_identifier` (so the two
same-name records don't collide), and a `health_check_id` pointing at that
region's health check. Route 53 serves the PRIMARY while its `health_check_id` is
healthy and switches to the SECONDARY when it isn't. Set
`evaluate_target_health = true` on the alias so an unhealthy NLB also triggers
failover, not just the standalone health check. See
[`route53_failover.tf`](../../examples/07-aws-containers/aws/route53_failover.tf).
</details>

<details><summary>Hint 2 — testing failover without breaking anything</summary>

You don't need to take down the primary region to test failover. Temporarily
**disable** (or invert) the primary's health check — in the console, "Invert
health check status", or detach the targets so the check goes unhealthy. Wait for
the failure threshold × request interval to elapse, then `dig` the name: it should
return the secondary. Re-enable the check to flip back. This exercises the routing
logic without any real outage.
</details>

## Stretch goals

- **Latency-based routing per region.** Instead of PRIMARY/SECONDARY, give each
  region's Frontend a latency record for the same name so Workers in each region
  resolve to their *nearest* Frontend (with health checks still pruning unhealthy
  ones). Geolocation routing is the variant when you want to pin by region rather
  than measured latency.
- **Pair this with Temporal multi-cluster replication for true cross-region DR.**
  DNS failover handles reachability; add Temporal's multi-cluster replication so
  the secondary cluster actually holds your Workflow history. Only with **both**
  do you have real cross-region durability — re-read the "what DNS failover does
  and does NOT give you" box in
  [`route53_records.md`](../../examples/07-aws-containers/aws/route53_records.md)
  and design the data half deliberately.
