# Lab 1.2b — Hello Temporal on a Dockerized cluster

**Time:** ~30 min · **Difficulty:** ★ · **Stack:** Temporal Server in Docker (auto-setup + PostgreSQL + UI)

## Scenario

Another variant of [Lab 1.2](lab-2-hello-temporal.md). So far you've run against
`temporal server start-dev` — one binary, in-memory state. Here you run the
**same Workflow** against a real multi-service cluster brought up with Docker
Compose: Frontend/History/Matching (the `temporalio/auto-setup` image) backed by
**PostgreSQL**, plus the standalone **Web UI** container.

The punchline: only the **environment** changes. Both this lab and the
[Cloud variant](lab-2-hello-temporal-cloud.md) build on one env-driven worker
that calls `Connections.fromEnv()`. For Docker there are no credentials and the
default address (`127.0.0.1:7233`) already points at the cluster — so you set
*nothing* and the plaintext branch just connects. What changes is the
*infrastructure* — separate processes, durable Postgres persistence, a
standalone UI — which is much closer to how Temporal looks in production.

> **Shared base with Lab 1.2c.** `Connections.fromEnv()` is the same helper the
> Cloud lab uses; Docker exercises its trivial **plaintext** branch, Cloud its
> **TLS + auth** branches. One worker, one helper, different env.

## Learning goals

- Stand up a real Temporal cluster (4 services + a database) with Docker Compose.
- See that the dev server and a Dockerized cluster are the **same gRPC contract**
  — the same env-driven worker connects to both with no new config.
- Understand what the single-binary dev server collapses: persistence and the UI
  are now their own containers.

> **Coming from Airflow `[airflow]`:** this is `SequentialExecutor` + SQLite
> (the dev server) vs. a `docker-compose` with a real metadata DB and a separate
> webserver. Same DAGs, sturdier plumbing.

## Prerequisites

- Lab 1.2 complete (you have a working Workflow/Activity/Worker).
- Docker + Docker Compose v2 (`docker compose version`). See [`Setup.md`](../../Setup.md).
- **Stop `make temporal` if it's running** — the Docker cluster binds the same
  host port `:7233`, so the dev server and this stack are mutually exclusive.

## Bring up the cluster

From the repo root:

```bash
make stack-temporal          # auto-setup + PostgreSQL + UI; waits for health
```

<details><summary>Under the hood — what <code>make stack-temporal</code> runs</summary>

```bash
docker compose -f docker/compose.temporal.yml up -d
# temporalio/auto-setup (Frontend/History/Matching) + PostgreSQL + temporalio/ui.
# gRPC on host :7233, Web UI on host :8233.
```

</details>

This runs [`docker/compose.temporal.yml`](../../docker/compose.temporal.yml).
First start pulls images and seeds the Postgres schema, so give it a minute.
Check it:

```bash
scripts/start-stack.sh temporal status   # all three services Up / healthy
temporal operator namespace list          # 'default' exists (auto-setup created it)
```

<details><summary>Under the hood — what <code>scripts/start-stack.sh temporal status</code> runs</summary>

```bash
docker compose -f docker/compose.temporal.yml ps
```

</details>

Web UI: <http://127.0.0.1:8233> (now served by the `temporalio/ui` container, not
the dev server).

## Tasks

1. Bring the stack up and confirm all three containers are healthy.
2. Run the shared env-driven worker. With no `TEMPORAL_*` variables set, its
   `Connections.fromEnv()` plaintext branch defaults to `127.0.0.1:7233` /
   namespace `default` — exactly the Docker cluster:

   ```bash
   make run-connect          # examples/runnable/01b-hello-temporal-anywhere
   ```

   <details><summary>Under the hood — what <code>make run-connect</code> runs</summary>

   ```bash
   cd examples/runnable/01b-hello-temporal-anywhere && mvn -q compile exec:java
   # Env-driven connection via Connections.fromEnv(). Reads:
   #   TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY, TEMPORAL_TLS_CERT, TEMPORAL_TLS_KEY
   # With none set it defaults to plaintext 127.0.0.1:7233 / namespace default.
   ```

   </details>

   (Your original Lab 1.2 worker, `make run-hello`, also works against Docker —
   it's the same gRPC contract. The point of `run-connect` is one worker that
   *also* reaches Cloud in Lab 1.2c.)
3. Find the execution in the Docker-backed Web UI under the `default` namespace.
4. **Prove persistence is real:** restart the cluster *without* wiping volumes
   and confirm the completed Workflow is still there — something the in-memory
   dev server can't promise.

   ```bash
   docker compose -f docker/compose.temporal.yml restart temporal
   temporal workflow list          # your execution is still listed
   ```

## Verification

```bash
make run-connect
```

Expected: the worker logs `Connecting to local server at 127.0.0.1:7233`, then
the greeting prints to stdout. Then:

```bash
temporal workflow list            # hello-temporal-demo, Status Completed
```

In the Web UI you'll see the usual `WorkflowExecutionStarted`, the
`ActivityTaskScheduled`/`Started`/`Completed` trio, and
`WorkflowExecutionCompleted` — identical to the dev-server run, because it's the
same server code, just deployed differently.

## Definition of done

- [ ] `make stack-temporal` brings up auto-setup + PostgreSQL + UI, all healthy.
- [ ] `make run-connect` connects via the plaintext branch and prints the greeting.
- [ ] The Workflow shows **Completed** in `temporal workflow list` and the UI.
- [ ] After `docker compose ... restart temporal`, the execution is still there
      (Postgres persistence, not in-memory).

## Teardown

```bash
scripts/start-stack.sh temporal down   # stops containers AND removes the volume
```

<details><summary>Under the hood — what <code>scripts/start-stack.sh temporal down</code> runs</summary>

```bash
docker compose -f docker/compose.temporal.yml down -v   # -v also deletes the Postgres volume
```

</details>

> `down` passes `-v`, so the Postgres volume is deleted and history is gone. Just
> want to pause? Use `docker compose -f docker/compose.temporal.yml stop`.

## Hints

<details><summary>Hint 1 — "connection refused" on :7233</summary>

Either the stack isn't healthy yet (first boot seeds the schema — watch
`scripts/start-stack.sh temporal logs`), or `make temporal` is still running and
owns the port. Only one server can bind `:7233`.
</details>

<details><summary>Hint 2 — the UI is empty / won't load</summary>

The UI container talks to `temporal:7233` over the compose network and serves on
container `:8080`, mapped to host `:8233`. Give `temporal` a few seconds to pass
its healthcheck — the UI `depends_on` it as `service_healthy`.
</details>

<details><summary>Hint 3 — do I need to set any env vars or change code?</summary>

No. The Docker cluster speaks the same gRPC API on the same port,
unauthenticated, namespace `default` — so `Connections.fromEnv()` takes its
plaintext branch with the default `127.0.0.1:7233` and connects. No `TEMPORAL_*`
variables, no code edit. (Contrast with
[Lab 1.2c](lab-2-hello-temporal-cloud.md), where you set credentials and the
helper takes its TLS + auth branch instead.)
</details>

## Stretch goals

- Scale the Worker tier: run two copies of `make run-connect`'s Worker against
  the same Task Queue and watch them share polling. The cluster doesn't care how
  many Workers connect.
- Open a `psql` shell into the `postgresql` container and find the `executions`
  table — see the event history you read in Lab 1.3 sitting in real rows.
- Compare startup: time `make temporal` vs `make stack-temporal`. The dev server
  wins on speed; the Docker cluster wins on fidelity to production. Know when to
  reach for each.
