# Lab 1.1: Local dev setup

**Time:** ~20 min · **Difficulty:** ★ · **Stack:** Temporal dev server

## Scenario

Before writing any Workflow code you need a running Temporal server, a working
JDK/Maven toolchain, and the Temporal CLI. This lab gets all three verified and
gives you the muscle memory for the three-terminal pattern you'll use every day.

## Learning goals

- Start the single-binary dev server and reach the Web UI.
- Confirm your JDK, Maven, and `temporal` CLI versions.
- Understand what the dev server bundles vs. a production deployment.

## Prerequisites

- JDK 17+ and Maven on `PATH`.
- Temporal CLI installed (see [`Setup.md`](../../Setup.md) for per-platform
  install, or `make setup-mac` / `make setup-ubuntu`).

## Tasks

1. **Verify your toolchain.** Run the repo's checker and confirm Java, Maven,
   and `temporal` are all found:

   ```bash
   make check
   ```

   <details><summary>Under the hood: what <code>make check</code> runs</summary>

   ```bash
   scripts/check-local.sh   # verifies Java, Maven, and the temporal CLI are on PATH
   ```

   </details>

   If anything is missing, fix it before continuing; later labs assume a green
   `make check`.

2. **Start the dev server** in its own terminal and leave it running:

   ```bash
   make temporal
   ```

   <details><summary>Under the hood: what <code>make temporal</code> runs</summary>

   ```bash
   temporal server start-dev \
     --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
   # gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
   # Override via env: TEMPORAL_HOST, TEMPORAL_PORT, TEMPORAL_UI_PORT, TEMPORAL_METRICS_PORT.
   ```

   </details>

   Read the startup banner. Note the gRPC address and the Web UI URL it prints.

3. **Open the Web UI** at <http://127.0.0.1:8233>. Find the **Namespaces**
   selector and confirm `default` exists. There are zero Workflows so far;
   that's expected.

4. **Talk to the server from the CLI.** In a third terminal:

   ```bash
   temporal operator namespace list
   temporal workflow list
   ```

   The second command returns an empty list. You've just confirmed the CLI can
   reach the Frontend service.

5. **Map the architecture to what you started.** The dev server is a single
   process that bundles the Frontend, History, Matching, and internal Worker
   services plus the Web UI and SQLite persistence. Write down (for yourself)
   which of those a *production* deployment would run as separate, scaled
   components.

## Definition of done

- [ ] `make check` reports Java, Maven, and Temporal CLI all present.
- [ ] `make temporal` is running and the Web UI loads.
- [ ] `temporal workflow list` succeeds (empty result is fine).
- [ ] You can name the four server services the dev binary bundles.

## Verification

```bash
temporal operator cluster health     # should report SERVING
```

## Hints

<details><summary>Hint 1: CLI can't connect</summary>

The CLI defaults to `127.0.0.1:7233`. If `make temporal` is running but the CLI
times out, confirm nothing else is bound to that port and that the server
terminal didn't exit. Use `temporal --address 127.0.0.1:7233 workflow list` to
be explicit.
</details>

<details><summary>Hint 2: port already in use</summary>

A previous dev server may still be running. Find and stop it, or start the new
one on alternate ports. `make temporal` runs in the foreground, so closing that
terminal stops the server.
</details>

## Stretch goals

- Start a **persistent** dev server (`make temporal-persistent`) and confirm
  Workflow history survives a server restart. Compare to the default in-memory
  behavior. Why does this matter for the rest of the week?
- Explore `temporal operator namespace describe default` and note the default
  retention period for closed Workflows.
