# Route 53 records for a stable, failover Temporal Frontend endpoint

> For **self-hosted** Temporal on EKS/ECS only. On Temporal Cloud you already
> have a managed global endpoint and need none of this.

One stable name — `temporal.internal.example.com:7233` — fronts a regional
Temporal Frontend NLB, with health-checked failover to a second region. Workers
and clients hold that name in `TEMPORAL_ADDRESS` and never learn the underlying
LB hostnames. See [`route53_failover.tf`](route53_failover.tf) for the Terraform.

## The record layout

| Name | Type | Routing policy | Set identifier | Health check | Target (alias) |
|---|---|---|---|---|---|
| `temporal.internal.example.com` | A (alias) | FAILOVER → PRIMARY | `primary-us-east-1` | `frontend_primary` | us-east-1 Frontend NLB |
| `temporal.internal.example.com` | A (alias) | FAILOVER → SECONDARY | `secondary-us-west-2` | `frontend_secondary` | us-west-2 Frontend NLB |
| `internal.example.com` | (private hosted zone) | — | — | — | associated with the Worker VPC(s) |

Both A records share the **same name**; `set_identifier` keeps them distinct and
`failover_routing_policy` decides which one Route 53 hands back. While the primary
health check is healthy, queries resolve to the PRIMARY NLB; when it goes
unhealthy, they resolve to the SECONDARY.

## How a Worker reaches the Frontend

```
                        Workers / clients (private subnets, both regions)
                                       |
                         TEMPORAL_ADDRESS = temporal.internal.example.com:7233
                                       |
                          [ private hosted zone: internal.example.com ]
                                       |
                          Route 53 FAILOVER (health-checked)
                          /                                  \
              health check OK                          primary UNHEALTHY
                     |                                          |
        PRIMARY  →  us-east-1 Frontend NLB        SECONDARY  →  us-west-2 Frontend NLB
                     |                                          |
        us-east-1 Temporal cluster                us-west-2 Temporal cluster
        (your Workflow history lives here)        (EMPTY unless multi-cluster
                                                   replication is configured!)
```

The split-horizon piece: the **private** hosted zone is what makes in-VPC queries
resolve to the *internal* NLB. A public zone for the same name (if any) is for
resolution from outside the VPC — keep the Frontend internal; there's rarely a
reason to expose `7233` publicly.

## The gRPC connection-caching caveat

The Temporal SDK opens **long-lived gRPC connections** to the Frontend. gRPC
resolves the name **once at connect time** and then holds connections to the
resolved IPs — it does **not** re-resolve DNS on every call. Consequences:

- **Live connections don't instantly honor a DNS flip.** When failover changes
  what the name resolves to, already-connected clients keep talking to the old
  IPs until those connections break and the SDK reconnects (re-resolving then).
- **Failover helps NEW connections and RECONNECTS**, not live, healthy ones. A
  freshly started Worker, or one whose connection just dropped, picks up the new
  (secondary) target. Existing healthy connections to a *reachable* primary stay.
- **NLB health is faster than DNS for in-region failures.** If a single Frontend
  instance dies, the **NLB** drops it from rotation in seconds — much faster than
  DNS, which is gated by health-check intervals + resolver/SDK caching (TTL).
  DNS failover is for losing the **whole region's** Frontend, not one instance.
- **Set sane TTLs.** Lower TTL = faster failover propagation, more queries. For
  alias-to-NLB records the alias tracks the LB, but resolvers/SDK still cache;
  don't expect sub-second cutover.

## What DNS failover does — and does NOT — give you

```
┌─────────────────────────────────────────────────────────────────────────┐
│  DNS FAILOVER (this file)            ✗ NOT the same as DATA REPLICATION   │
├─────────────────────────────────────────────────────────────────────────┤
│  GIVES YOU                            DOES NOT GIVE YOU                    │
│  • a stable name decoupled from LBs   • Workflow history in the other     │
│  • clients can REACH *a* Frontend       region (secondary cluster is      │
│    in the secondary region              EMPTY without replication)        │
│  • automatic cutover for NEW            • cross-region durability / DR     │
│    connections when primary's          • instant cutover of LIVE gRPC     │
│    health check fails                    connections                      │
├─────────────────────────────────────────────────────────────────────────┤
│  For true cross-region DR, pair this with Temporal MULTI-CLUSTER          │
│  REPLICATION (a Temporal feature) so the secondary cluster actually holds │
│  your Workflows. DNS gets clients to a Frontend; replication is what makes │
│  the data be there. Solve BOTH, not just the DNS half.                    │
└─────────────────────────────────────────────────────────────────────────┘
```
