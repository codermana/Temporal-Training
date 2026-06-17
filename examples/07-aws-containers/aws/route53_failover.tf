# Route 53 failover for a SELF-HOSTED Temporal Frontend.
#
# Goal: give Workers and clients ONE stable DNS name —
# temporal.internal.example.com:7233 — that fronts a regional Temporal Frontend
# NLB, with health-checked PRIMARY/SECONDARY failover to a second region.
#
# WHO NEEDS THIS: self-hosters running Temporal on EKS/ECS. On Temporal Cloud
# you already have a managed global endpoint (<ns>.<acct>.tmprl.cloud:7233) —
# you do NOT need any of this. This is purely for self-hosted Frontends.
#
# THE CAVEAT THAT MATTERS MOST (read before trusting this):
#   DNS failover gives you frontend REACHABILITY across regions. It does NOT
#   replicate Workflow state. The SECONDARY region's Frontend talks to the
#   SECONDARY region's Temporal cluster, which has NONE of the primary's history
#   unless you run Temporal MULTI-CLUSTER REPLICATION (a Temporal feature). So:
#       DNS failover  = "clients can reach *a* Frontend"
#       replication   = "the other region actually has your Workflows' data"
#   These are different problems. This file solves the first one only.
#
# COST: a hosted zone is ~$0.50/mo; each health check is ~cents/mo. Cheap, but
# NOT free-LocalStack territory (LocalStack can't do real Route 53 hosted zones
# or health checks meaningfully). Apply only with a real account + domain.
#
# Placeholders below — region us-east-1 (primary) / us-west-2 (secondary), zone
# id Z..., and NLB DNS names — must be replaced with your real values.

# --------------------------------------------------------------------------- #
# Private hosted zone — internal, VPC-scoped resolution.                       #
# --------------------------------------------------------------------------- #
# Workers live in PRIVATE subnets and should resolve temporal.internal.example.com
# to the INTERNAL NLB addresses, never a public IP. A PRIVATE hosted zone
# associated with the Worker VPC(s) does exactly that.
#
# SPLIT-HORIZON note: you can run a private zone AND a public zone for the same
# name. In-VPC queries hit the private zone (internal NLB); anything outside sees
# the public zone (or nothing). Keep the Frontend internal — there is rarely a
# reason to expose 7233 to the public internet.
resource "aws_route53_zone" "internal" {
  name = "internal.example.com"

  vpc {
    vpc_id = "vpc-0aaaa1111bbbb2222" # the VPC where Workers/clients run (us-east-1)
  }

  # Associate every VPC that must resolve the name (e.g. the us-west-2 Worker VPC)
  # with additional `vpc { ... }` blocks or aws_route53_zone_association resources.

  comment = "Internal name for the self-hosted Temporal Frontend; resolves to regional NLBs."
}

# --------------------------------------------------------------------------- #
# Health checks — one per regional Frontend.                                   #
# --------------------------------------------------------------------------- #
# Route 53 only fails over if it can TELL the primary is unhealthy. A health
# check probes each region's Frontend; FAILOVER routing (below) keys off it.
#
# IMPORTANT: the check must reflect *Frontend* health, not just "a TCP port is
# open". A plain TCP check on 7233 goes green the instant the NLB has any
# registered target — even one whose Temporal cluster is degraded. Prefer a
# check that exercises the Frontend's actual health endpoint (the gRPC health
# service, or an HTTP health sidecar) so "healthy" means "can serve Workflows".
#
# Health checks probe PUBLIC endpoints by default. For a fully private Frontend,
# use a CloudWatch-alarm-backed health check (calculated/alarm type) driven by an
# NLB target-health or Temporal frontend metric, since Route 53's global checkers
# can't reach a private NLB. (Shown as a comment on the primary below.)

resource "aws_route53_health_check" "frontend_primary" {
  # If the Frontend health endpoint is reachable by Route 53's checkers:
  fqdn              = "frontend-primary.us-east-1.example.com" # the primary NLB / health endpoint
  port              = 7233
  type              = "TCP" # prefer HTTPS/gRPC health if you have an HTTP health sidecar
  failure_threshold = 3     # consecutive failures before "unhealthy"
  request_interval  = 10    # seconds between probes (10 or 30)

  # For a PRIVATE Frontend, drop fqdn/port/type above and instead point at a
  # CloudWatch alarm (e.g. NLB UnHealthyHostCount or a custom frontend metric):
  #   type                            = "CLOUDWATCH_METRIC"
  #   cloudwatch_alarm_name           = "temporal-frontend-primary-unhealthy"
  #   cloudwatch_alarm_region         = "us-east-1"
  #   insufficient_data_health_status = "Unhealthy"

  tags = { Name = "temporal-frontend-primary", Region = "us-east-1" }
}

resource "aws_route53_health_check" "frontend_secondary" {
  fqdn              = "frontend-secondary.us-west-2.example.com" # the secondary NLB / health endpoint
  port              = 7233
  type              = "TCP"
  failure_threshold = 3
  request_interval  = 10

  tags = { Name = "temporal-frontend-secondary", Region = "us-west-2" }
}

# --------------------------------------------------------------------------- #
# Failover records — the stable name, with PRIMARY/SECONDARY routing.          #
# --------------------------------------------------------------------------- #
# Two records share the SAME name (temporal.internal.example.com) but differ by
# `set_identifier` and `failover_routing_policy`. Route 53 returns the PRIMARY
# while its health check is healthy; when the primary check goes unhealthy it
# returns the SECONDARY. `alias` points each at its regional NLB so the record
# tracks the NLB even if the LB is recreated — the name stays put, which is the
# whole point: TEMPORAL_ADDRESS never has to change.
#
# TTL note: alias records to an NLB don't take a literal TTL field, but resolvers
# and the SDK's connection still cache results. Failover affects NEW resolutions
# / reconnects, not live connections (see route53_records.md, the gRPC caveat).

resource "aws_route53_record" "temporal_primary" {
  zone_id        = aws_route53_zone.internal.zone_id
  name           = "temporal.internal.example.com"
  type           = "A"
  set_identifier = "primary-us-east-1" # distinguishes the two same-name records

  failover_routing_policy {
    type = "PRIMARY"
  }

  # Bind this record to the primary health check — when it goes unhealthy,
  # Route 53 stops returning this record and serves the SECONDARY instead.
  health_check_id = aws_route53_health_check.frontend_primary.id

  alias {
    name                   = "primary-nlb-0a1b2c3d4e5f6a7b.elb.us-east-1.amazonaws.com" # the us-east-1 Frontend NLB
    zone_id                = "Z26RNL4JYFTOTI"                                            # NLB hosted-zone id for us-east-1
    evaluate_target_health = true                                                        # also fail over if the NLB itself reports unhealthy targets
  }
}

resource "aws_route53_record" "temporal_secondary" {
  zone_id        = aws_route53_zone.internal.zone_id
  name           = "temporal.internal.example.com"
  type           = "A"
  set_identifier = "secondary-us-west-2"

  failover_routing_policy {
    type = "SECONDARY"
  }

  health_check_id = aws_route53_health_check.frontend_secondary.id

  alias {
    name                   = "secondary-nlb-7b6a5f4e3d2c1b0a.elb.us-west-2.amazonaws.com" # the us-west-2 Frontend NLB
    zone_id                = "Z18D5FSROUN65G"                                             # NLB hosted-zone id for us-west-2
    evaluate_target_health = true
  }
}

# --------------------------------------------------------------------------- #
# What to do with this name.                                                   #
# --------------------------------------------------------------------------- #
# Point every Worker and client at the stable name, NOT a region-specific LB:
#     TEMPORAL_ADDRESS=temporal.internal.example.com:7233
# Now the LB can be recreated, or the primary region can fail its health check,
# and the address your config holds never changes.
output "temporal_address" {
  description = "Stable endpoint for TEMPORAL_ADDRESS — survives LB recreation and fails over to the secondary region."
  value       = "${aws_route53_record.temporal_primary.name}:7233"
}

# REMINDER (again, because it's the trap): this gives clients a reachable
# Frontend in another region. It does NOT give that region your Workflow data.
# For true cross-region durability/DR, pair this with Temporal multi-cluster
# replication so the secondary cluster actually holds the history.
