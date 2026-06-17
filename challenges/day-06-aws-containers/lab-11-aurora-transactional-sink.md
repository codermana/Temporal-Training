# Lab 6.11: Aurora as the transactional sink

**Time:** ~50 min · **Difficulty:** ★★★ · **Stack:** real AWS (Aurora/RDS), optional

> **Optional · real AWS account required.** Provisions a real Aurora cluster
> (RDS, costs ~cents+/hr). **Not** runnable on free LocalStack: there are **no
> `make` targets**. Walk the pattern and the schema; the *idempotency* lesson is
> the takeaway and is testable locally against any Postgres. Apply the Aurora
> bits only with an account.

## Scenario

The import pipeline's final step, `load`, persists rows into a relational store,
Aurora (PostgreSQL-compatible). In AWS this is a Glue job or a Lambda that
`INSERT`s the batch into RDS, and you cross your fingers the orchestrator never
re-runs it. Temporal makes the re-run an **expected** event: Activities are
**at-least-once**, so `load` *will* fire twice eventually: a retry, a
`startToCloseTimeout`, or a Worker crash mid-batch. Your job is to make a
duplicated `load` Activity **not** double-insert: a stable idempotency key plus
`INSERT ... ON CONFLICT DO NOTHING`, all in one transaction. **at-least-once
Activity + idempotent SQL = effectively-once writes.**

> "Activities follow an at-least-once execution model." … "Because Activities may be retried due to failures, it's strongly recommended to make them idempotent."
>
> from *Temporal Documentation*, docs.temporal.io

<!-- source: https://docs.temporal.io/develop/python/best-practices/error-handling -->

## Learning goals

- Why Temporal Activities are at-least-once, and why idempotent SQL turns that
  into **effectively-once** writes.
- The idempotency-key pattern in a **relational** store: a `UNIQUE` constraint +
  `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING`, wrapped in a transaction.
- Choosing a **stable** idempotency key (derived from `workflowId`, not from
  wall-clock or random) so it survives retries.
- **Connection management in Workers:** a Worker is many concurrent Activity
  threads → a bounded connection **pool** (HikariCP/pgbouncer), sized relative to
  activity concurrency.
- Credentials from **SSM / IAM DB auth** (no password in code), and writing to
  the Aurora **writer** endpoint (reader is for queries).

## What you'll need

Either path works; the idempotency lesson is the same:

- **Real AWS:** an account, and an Aurora PostgreSQL cluster stood up from
  [`examples/07-aws-containers/aws/aurora_terraform.tf`](../../examples/07-aws-containers/aws/aurora_terraform.tf)
  (`terraform apply`, then read the `writer_endpoint` output). Costs ~cents+/hr.
- **Local (recommended to learn the pattern):** any Postgres
  (`docker run -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:15`). The
  idempotency logic (schema, `ON CONFLICT`, the transaction) is identical;
  only the endpoint and auth differ. `make temporal` for the Temporal server.

The three reference files:
[`aurora_schema.sql`](../../examples/07-aws-containers/aws/aurora_schema.sql),
[`aurora_load_activity.java`](../../examples/07-aws-containers/aws/aurora_load_activity.java),
[`aurora_terraform.tf`](../../examples/07-aws-containers/aws/aurora_terraform.tf).

## The pattern

**1. `aurora_schema.sql`: the dedupe lives in the schema.** A `loaded_rows`
table carries an `idempotency_key TEXT` column with a `UNIQUE` constraint. That
constraint is *what makes* `ON CONFLICT (idempotency_key) DO NOTHING` a no-op on
the second attempt; without it, the conflict target doesn't exist and the
duplicate insert succeeds. The file also shows the `import_dedup` variant (a
dedicated dedup table you claim first, the SQL twin of the DynamoDB
`attribute_not_exists(pk)` put) and `DO NOTHING` vs `DO UPDATE`.

**2. `aurora_load_activity.java`: the idempotent `load` Activity.** It takes a
pooled connection, turns **off** auto-commit, runs the whole batch as one
`INSERT ... ON CONFLICT DO NOTHING` between `BEGIN`/`COMMIT`, and returns the
count of rows actually inserted. The key is `workflowId + ":" + batchIndex`: the
**same** input yields the **same** key on every attempt, so a retry conflicts
harmlessly. The pool (HikariCP) is built **once** at Worker startup, sized
`~ maxConcurrentActivityExecutionSize`, with credentials from SSM / IAM auth.

**3. `aurora_terraform.tf`: the cluster.** An `aws_rds_cluster`
(engine `aurora-postgresql`) + an `aws_rds_cluster_instance`, with
`iam_database_authentication_enabled = true` and the master password sourced from
Secrets Manager (never a literal). Note the two endpoints: `endpoint` (writer,
used by `load`) and `reader_endpoint` (queries only).

### Aurora can also be Temporal's *own* persistence store

A short aside, not the focus of this lab: when you **self-host** Temporal (e.g. on
EKS rather than Temporal Cloud), the Temporal *server* needs a database, and
Aurora PostgreSQL is a supported persistence backend. That's the
"Temporal Cloud vs EKS self-hosted" tradeoff: Cloud manages persistence for you;
self-hosting means you operate an Aurora cluster for Temporal's own history. Don't
conflate the two roles: **this lab is about Aurora as the *application* sink**
(where `load` writes business rows), not as Temporal's internal store.

## Coming from AWS

> In the AWS pipeline a Lambda or Glue job `INSERT`s the batch into RDS and you
> *pray* the orchestrator doesn't double-run it: you bolt on a "have I run this
> before?" check against a marker table, or you accept occasional duplicates.
> With Temporal you stop praying: an **at-least-once Activity** is *expected* to
> re-run, and `INSERT ... ON CONFLICT (idempotency_key) DO NOTHING` makes that
> re-run a no-op: **effectively-once** by construction.

This is the **relational sibling** of the NoSQL idempotency pattern. The existing
DynamoDB examples,
[`java`](../../examples/07-aws-containers/java/dynamodb_idempotency.java),
[`python`](../../examples/07-aws-containers/python/dynamodb_idempotency.py),
[`go`](../../examples/07-aws-containers/go/dynamodb_idempotency.go),
do the same thing with a conditional put (`attribute_not_exists(pk)`). DynamoDB's
conditional write *is* Postgres's `ON CONFLICT`; the unique key *is* the
partition key. Same lesson, different store.

## Tasks

1. **Define the schema.** Apply `aurora_schema.sql` to your Aurora (or local)
   Postgres: the `loaded_rows` table with the `UNIQUE (idempotency_key)`
   constraint.
2. **Implement the idempotent `load`.** Port `aurora_load_activity.java`: pooled
   connection, auto-commit off, batched `INSERT ... ON CONFLICT DO NOTHING` keyed
   on `workflowId + ":" + batchIndex`, `COMMIT`, return the inserted-row count.
3. **Drive a Workflow** that calls `load` with a fixed batch (reuse
   `ImportWorkflow` if you've built it).
4. **Force a retry and prove no duplicates.** Kill the Worker mid-`load`
   (or throw *after* the inserts, *before* `commit()`) so Temporal retries the
   Activity. Confirm the row count is **identical** before and after the retry.

## Verification

```sql
SELECT count(*) FROM loaded_rows WHERE idempotency_key LIKE '<your-workflow-id>:%';
```

- The count is **the same** after the forced retry as after the first run: the
  `UNIQUE` constraint + `ON CONFLICT DO NOTHING` blocked the second insert.
- In the Temporal **Web UI**, the `load` Activity shows an `ActivityTaskFailed`
  followed by a retry that succeeds: visible proof the Activity ran twice while
  the data did not duplicate.

## Definition of done

- [ ] The schema has a `UNIQUE` constraint on the idempotency key.
- [ ] The `load` Activity wraps the batch in one transaction (auto-commit off)
      and uses `ON CONFLICT (idempotency_key) DO NOTHING`.
- [ ] The idempotency key is **stable across attempts** (derived from
      `workflowId`, not wall-clock/random).
- [ ] A forced retry leaves `count(*)` unchanged; the UI shows the retry.
- [ ] Credentials come from SSM / IAM auth and the pool is bounded: no password
      in code, no connection-per-Activity.

## Pitfalls

- **Auto-commit per row defeats the transaction.** If each `INSERT` commits on
  its own, a mid-batch crash leaves the batch half-loaded and you've lost the
  atomicity entirely. Turn auto-commit **off** and `COMMIT` once.
- **A non-deterministic idempotency key dedupes nothing.** Derive it from
  `workflowId` (stable across attempts). Key it on `System.currentTimeMillis()`
  or a random UUID and every retry looks "new": straight back to duplicates.
- **An unbounded pool exhausts Aurora.** A Worker has many Activity threads;
  multiply by replicas and an unbounded pool blows past `max_connections`. Size
  HikariCP `maximumPoolSize` relative to `maxConcurrentActivityExecutionSize`,
  and keep the sum across replicas under the cluster limit.
- **Password in code.** Read it from SSM / Secrets Manager, or use an IAM DB auth
  token, never a literal in source or the image (forward-ref
  [Lab 8: SSM Parameter Store](lab-8-ssm-parameter-store.md)).

## Cost

Aurora bills by the hour while it's up. When you're done:

```bash
terraform destroy   # tear down the cluster, don't leave it running
```

The local-Postgres path costs nothing; use it to drill the idempotency logic.

## Hints

<details><summary>Hint 1: the idempotency key</summary>

Use `idempotencyKey = workflowId + ":" + batchIndex` (e.g.
`"import-2026-06-16:0"`). It's deterministic from the Workflow, so attempt 2
produces the exact same keys as attempt 1 and every row conflicts harmlessly.
Never fold in `now()` or `random()`.
</details>

<details><summary>Hint 2: DO NOTHING vs DO UPDATE</summary>

`ON CONFLICT ... DO NOTHING` for an append-only load: a retry must be a pure
no-op. `ON CONFLICT ... DO UPDATE SET ...` (an UPSERT) when a re-run should
*refresh* a mutable column (e.g. a status) while still keyed on the same stable
key; see the bottom of `aurora_schema.sql`.
</details>

## Stretch goals

- **IAM DB auth token instead of a password.** Generate a short-lived token
  (`RdsUtilities.generateAuthenticationToken`) at connection time so there's no
  stored secret at all; `iam_database_authentication_enabled` is already set in
  the Terraform.
- **Idempotent UPSERT.** Switch to `ON CONFLICT ... DO UPDATE` that updates a
  `status` column on the dedup row, so a re-run advances state
  (`IN_PROGRESS` → `DONE`) instead of being a flat no-op.
