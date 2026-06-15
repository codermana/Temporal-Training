# Lab 1.2c — Hello Temporal on Temporal Cloud

**Time:** ~40 min · **Difficulty:** ★★ · **Stack:** Temporal Cloud

## Scenario

This is a variant of [Lab 1.2](lab-2-hello-temporal.md). The Workflow, Activity,
and Worker are **identical** — the only thing that changes is *where the client
connects*. In Lab 1.2 you pointed at a local dev server with
`WorkflowServiceStubs.newLocalServiceStubs()`. Here you connect to **Temporal
Cloud**, the managed service, which adds the two things a production cluster
requires: a **TLS endpoint** and **authentication** (an API key or an mTLS
client certificate).

The goal is to internalize that, in Temporal, your business code is decoupled
from the cluster. Moving from laptop to Temporal Cloud is a connection change,
not a rewrite.

## Learning goals

- Connect a Worker and client to **Temporal Cloud** over TLS.
- Authenticate with either an **API key** or an **mTLS client certificate**.
- Set your Cloud **namespace** explicitly (no more implicit `default`).
- Run the *same* Workflow code against Cloud that you ran locally.

> **Coming from Airflow `[airflow]`:** this is the equivalent of repointing your
> scheduler/workers from a local Postgres+executor to a managed Airflow
> (MWAA/Astronomer). Your DAGs don't change — connection string and credentials
> do.

## Prerequisites

- Lab 1.2 complete (you have a working Workflow/Activity/Worker).
- A **Temporal Cloud** account with a **namespace** created
  (<https://cloud.temporal.io>). If you don't have Cloud access yet, the
  [shared runnable module](../../examples/runnable/01b-hello-temporal-anywhere)
  falls back to a local server, so you can still build and study the code.
- One set of credentials for your Cloud namespace:
  - **API key** (recommended — simplest): create one in the Cloud UI under
    *Settings → API Keys*, or `temporal cloud apikey create --name hello --duration 24h`.
  - **mTLS**: a client certificate + private key pair (PKCS#8) whose CA is
    attached to the namespace. See `temporal cloud namespace` docs.

## What changes vs. Lab 1.2

Only the connection. In Lab 1.2:

```java
WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
WorkflowClient client = WorkflowClient.newInstance(service);
```

For Temporal Cloud you build `WorkflowServiceStubsOptions` (target + TLS + auth)
and set your Cloud namespace on the client. Everything below the connection —
`WorkerFactory`, `registerWorkflowImplementationTypes`, the typed stub, `greet`
— stays byte-for-byte the same.

> **Shared base with Lab 1.2b.** Both this lab and the
> [Docker variant](lab-2-hello-temporal-docker.md) build on the *same*
> `Connections.fromEnv()` helper — a one-time refactor of Lab 1.2 that picks the
> connection from environment variables. The Docker/local case is the trivial
> **plaintext** branch (just a target). This lab implements the interesting
> branches: **API key** and **mTLS**. Same worker, same helper — only the
> environment differs.

## Starter code

Refactor your Lab 1.2 worker once to read its connection from the environment
(the `pom.xml` is unchanged — `temporal-sdk` already bundles everything needed
for TLS and API keys). Keep credentials out of source by reading them from env.

**`Connections.java`** — the shared helper. The plaintext branch is already done
(that's what Lab 1.2b uses); fill in the three Cloud TODOs:

```java
package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.FileInputStream;
import java.io.InputStream;

public final class Connections {
  private Connections() {}

  public static WorkflowClient fromEnv() {
    String address = getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233");
    String namespace = getenv("TEMPORAL_NAMESPACE", "default");
    String apiKey = System.getenv("TEMPORAL_API_KEY");
    String tlsCert = System.getenv("TEMPORAL_TLS_CERT");
    String tlsKey = System.getenv("TEMPORAL_TLS_KEY");

    WorkflowServiceStubsOptions.Builder stubs = WorkflowServiceStubsOptions.newBuilder();

    if (apiKey != null && !apiKey.isBlank()) {
      // TODO 1 (API key): set the target to `address`, enable HTTPS,
      //   and add the API key with stubs.addApiKey(() -> apiKey).
    } else if (tlsCert != null && tlsKey != null) {
      // TODO 2 (mTLS): build an SslContext from the PKCS#8 cert + key and set it.
      //   try (InputStream cert = new FileInputStream(tlsCert);
      //        InputStream key  = new FileInputStream(tlsKey)) {
      //     stubs.setSslContext(SimpleSslContextBuilder.forPKCS8(cert, key).build());
      //   }
      //   stubs.setTarget(address);
    } else {
      stubs.setTarget(address); // local fallback, plaintext
    }

    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubs.build());

    // TODO 3: return a WorkflowClient built with WorkflowClientOptions that
    //   sets the namespace (Cloud namespaces look like `my-ns.a1b2c`).
    throw new UnsupportedOperationException("TODO");
  }

  private static String getenv(String key, String fallback) {
    String v = System.getenv(key);
    return (v != null && !v.isBlank()) ? v : fallback;
  }
}
```

Then change one line in your worker — swap the local-stubs block for:

```java
WorkflowClient client = Connections.fromEnv();
```

(See the [shared runnable module](../../examples/runnable/01b-hello-temporal-anywhere)
for the completed `Connections.java` and `HelloWorker.java` — the same module
Lab 1.2b uses.)

## Tasks

1. Complete the three TODOs in `Connections.java`.
2. Wire `Connections.fromEnv()` into your worker, replacing
   `WorkflowServiceStubs.newLocalServiceStubs()`.
3. Export the right environment variables for your auth method (below) and run.
4. Confirm the greeting prints **and** that the execution shows up in the
   **Cloud** Web UI for your namespace — not your local UI.

### Environment — API key

```bash
export TEMPORAL_ADDRESS="us-east-1.aws.api.temporal.io:7233"   # your region's gRPC endpoint
export TEMPORAL_NAMESPACE="my-namespace.a1b2c"                 # <namespace>.<account>
export TEMPORAL_API_KEY="$(cat ~/.temporal/hello.key)"        # never hard-code this
```

### Environment — mTLS

```bash
export TEMPORAL_ADDRESS="my-namespace.a1b2c.tmprl.cloud:7233"  # namespace gRPC endpoint
export TEMPORAL_NAMESPACE="my-namespace.a1b2c"
export TEMPORAL_TLS_CERT="/path/to/client.pem"
export TEMPORAL_TLS_KEY="/path/to/client.key"
```

## Verification

```bash
# from your module (or examples/runnable/01b-hello-temporal-anywhere)
mvn -q compile exec:java         # or, from the repo root: make run-connect
```

Expected: the log prints which mode it connected with, then the greeting string.

Point the CLI at the same namespace to see the execution:

```bash
# API key
temporal workflow list \
  --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY"

# mTLS
temporal workflow list \
  --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" \
  --tls-cert-path "$TEMPORAL_TLS_CERT" --tls-key-path "$TEMPORAL_TLS_KEY"
```

Your `hello-temporal-demo` workflow appears with Status **Completed**, and the
same execution is visible in the Temporal Cloud Web UI.

## Definition of done

- [ ] The Worker connects to the remote namespace over TLS (the run log shows
      "API key" or "mTLS", not "local").
- [ ] Running the program prints the greeting.
- [ ] The execution is **Completed** in the Cloud namespace (CLI + Cloud UI),
      and does **not** appear in your local dev server's UI.
- [ ] No credentials are hard-coded — everything comes from the environment.

## Hints

<details><summary>Hint 1 — API key connection refused / UNAUTHENTICATED</summary>

API keys require TLS: you must call `setEnableHttps(true)`. The `address` for
API keys is the **regional** endpoint (e.g. `us-east-1.aws.api.temporal.io:7233`),
not the `*.tmprl.cloud` namespace endpoint used for mTLS. The namespace still
has to be set on the `WorkflowClient` via `WorkflowClientOptions`.
</details>

<details><summary>Hint 2 — namespace not found</summary>

A Cloud namespace identifier includes the account suffix:
`my-namespace.a1b2c`, not just `my-namespace`. Set it with
`WorkflowClientOptions.newBuilder().setNamespace(...)`. Forgetting it leaves you
on `default`, which doesn't exist in Cloud.
</details>

<details><summary>Hint 3 — mTLS handshake / "no private key" error</summary>

`SimpleSslContextBuilder.forPKCS8(certStream, keyStream)` expects a **PKCS#8**
private key. If your key is PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`), convert
it: `openssl pkcs8 -topk8 -nocrypt -in client.key -out client.pk8.key`.
</details>

## Stretch goals

- Run a Worker against Cloud while a *second* Worker on the same Task Queue runs
  locally against `make temporal`. Confirm they're fully isolated — different
  namespaces, different histories.
- Add a `temporal-namespace` header check: connect with the wrong namespace and
  read the gRPC error. Understand why the namespace is part of routing, not just
  authorization.
- Replace the long-lived API key with a short-duration one
  (`temporal cloud apikey create --duration 1h`) and observe the Worker losing
  auth after expiry — motivation for `addApiKey(Supplier)` re-reading a rotating
  source rather than a captured string.
