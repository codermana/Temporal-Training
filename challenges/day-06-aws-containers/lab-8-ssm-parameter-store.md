# Lab 6.8 — Worker config & secrets from SSM Parameter Store

**Time:** ~40 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

Your Worker still hardcodes its Temporal address, namespace, and task queue, and
the downstream API key rides in as a plaintext env var. That doesn't survive a
move to containers: the image shouldn't carry secrets and config shouldn't be
rebuilt to change. You'll load the Worker's config from **SSM Parameter Store at
startup**, decrypt the **SecureString** secret with KMS, and read the per-run API
key **inside an Activity** so the Workflow never touches non-deterministic I/O.
LocalStack stands in for SSM.

## Learning goals

- Bootstrap config lives in **process startup code, not Workflow code** — an SSM
  read is non-deterministic and can change between replays.
- A secret accessed at **use-time** belongs in an **Activity**, not the Workflow.
- `SecureString` parameters + `WithDecryption` (KMS) vs. plain `String` config.
- How this maps onto **ECS task roles / EKS IRSA**: the SSM client authenticates
  from the runtime identity, so there are **no static keys** in the image.

> "SecureString parameters encrypt values using AWS Key Management Service, making them a practical choice for lightweight encrypted configuration values that don't require rotation or other advanced secret lifecycle capabilities."
>
> — *AWS Systems Manager User Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html -->

## Prerequisites

```bash
make temporal      # terminal 1
make stack-aws     # terminal 2: LocalStack on :4566
```

<details><summary>Under the hood — what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
# gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
# Overridable via TEMPORAL_HOST, TEMPORAL_PORT, TEMPORAL_UI_PORT, TEMPORAL_METRICS_PORT.
```

</details>

<details><summary>Under the hood — what <code>make stack-aws</code> runs</summary>

```bash
docker compose -f docker/compose.localstack.yml up -d
# LocalStack S3/SQS/Glue/SSM on :4566
```

</details>

Seed the parameter tree (a couple of `String` config values plus one
`SecureString` secret):

```bash
awslocal ssm put-parameter --name /temporal-training/worker/temporal-address \
  --value 127.0.0.1:7233 --type String
awslocal ssm put-parameter --name /temporal-training/worker/task-queue \
  --value transform --type String
awslocal ssm put-parameter --name /temporal-training/worker/order-api-url \
  --value http://orders.internal/v1 --type String
awslocal ssm put-parameter --name /temporal-training/worker/api-key \
  --value sk-live-DO-NOT-COMMIT --type SecureString
```

> **Coming from AWS:** the old pattern is **env vars / a mounted secrets file**
> baked into the task definition. Here it becomes **SSM Parameter Store read at
> startup** for config, and **a secret fetched inside an Activity** at use-time.
> In ECS/EKS the SSM client authenticates via the **task role / IRSA** — no
> static access keys travel in the image.

## Starter code

Extend the `training.temporal.aws` module. `pom.xml` adds AWS SDK v2 SSM:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>ssm</artifactId>
  <version>2.25.0</version>
</dependency>
```

**Config-loader skeleton — complete the TODOs:**

```java
public class WorkerBootstrap {

  // Immutable snapshot, cached at boot — read SSM once, not per task.
  record WorkerConfig(String temporalAddress, String namespace,
                      String taskQueue, String orderApiUrl) {}

  static SsmClient ssmForLocalStack() {
    // TODO 1: build an SsmClient pointed at LocalStack (endpoint
    //   http://127.0.0.1:4566, region us-east-1, dummy test/test creds).
    //   In ECS/EKS you'd drop the override and let the task role / IRSA supply creds.
    throw new UnsupportedOperationException("TODO");
  }

  static WorkerConfig loadConfig(SsmClient ssm, String path) {
    // TODO 2: getParametersByPath(path, recursive=true, withDecryption=true);
    //   follow NextToken until null; key each param by its trailing name
    //   ("/temporal-training/worker/namespace" -> "namespace").
    // TODO 3: map the collected values into a WorkerConfig record.
    throw new UnsupportedOperationException("TODO");
  }

  static WorkerFactory bootstrap() {
    // TODO 4: read config from SSM (startup code — this is allowed here),
    //   build WorkflowServiceStubs(target=temporalAddress) + WorkflowClient
    //   (namespace), then a WorkerFactory + Worker on taskQueue.
    throw new UnsupportedOperationException("TODO");
  }
}
```

**`fetchApiKey` Activity skeleton — read the SecureString at use-time:**

```java
@ActivityInterface
public interface SecretActivities {
  @ActivityMethod
  String fetchApiKey();
}

public class SecretActivitiesImpl implements SecretActivities {
  private final SsmClient ssm;
  public SecretActivitiesImpl(SsmClient ssm) { this.ssm = ssm; }

  @Override
  public String fetchApiKey() {
    // TODO: getParameter("/temporal-training/worker/api-key", withDecryption=true)
    //   and return the value. NEVER read this from Workflow code; NEVER log it.
    throw new UnsupportedOperationException("TODO");
  }
}
```

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference ports: [`examples/07-aws-containers/python/ssm_parameter_config.py`](../../examples/07-aws-containers/python/ssm_parameter_config.py)
and [`.../go/ssm_parameter_config.go`](../../examples/07-aws-containers/go/ssm_parameter_config.go).
Try the TODOs yourself before peeking. (`boto3` / `aws-sdk-go-v2` may be absent
offline — the load-at-boot + secret-in-an-Activity split is the deliverable.)

**Python** (`temporalio`) — config loaded at boot, secret behind an Activity:

```python
from dataclasses import dataclass
from temporalio import activity

@dataclass(frozen=True)
class WorkerConfig:
    temporal_address: str
    namespace: str
    task_queue: str
    order_api_url: str

def load_config(path: str = "/temporal-training/worker/") -> WorkerConfig:
    ssm = boto3.client("ssm")  # import boto3 lazily; LocalStack endpoint
    params, next_token = {}, None
    while True:
        kwargs = {"Path": path, "Recursive": True, "WithDecryption": True}
        if next_token: kwargs["NextToken"] = next_token
        resp = ssm.get_parameters_by_path(**kwargs)
        for p in resp["Parameters"]:
            params[p["Name"].rsplit("/", 1)[-1]] = p["Value"]   # key by trailing name
        next_token = resp.get("NextToken")
        if not next_token: break
    return WorkerConfig(params["temporal-address"], params.get("namespace", "default"),
                        params["task-queue"], params["order-api-url"])

@activity.defn
async def fetch_api_key() -> str:                      # read at use-time, in an Activity
    ssm = boto3.client("ssm")
    return ssm.get_parameter(Name="/temporal-training/worker/api-key",
                             WithDecryption=True)["Parameter"]["Value"]
```

```python
# bootstrap (process startup — reading SSM here is fine):
cfg = load_config()
client = await Client.connect(cfg.temporal_address, namespace=cfg.namespace)
worker = Worker(client, task_queue=cfg.task_queue, activities=[fetch_api_key])
```

**Go** (`go.temporal.io/sdk`) — same split; SSM behind a small interface:

```go
func LoadConfig(ctx context.Context, ssm SsmAPI, path string) (WorkerConfig, error) {
    params, err := ssm.GetByPath(ctx, path, true) // withDecryption=true, paginates inside
    if err != nil { return WorkerConfig{}, err }
    ns := params["namespace"]; if ns == "" { ns = "default" }
    return WorkerConfig{params["temporal-address"], ns,
        params["task-queue"], params["order-api-url"]}, nil
}

func (a *SecretActivities) FetchAPIKey(ctx context.Context) (string, error) {
    return a.Ssm.GetOne(ctx, "/temporal-training/worker/api-key", true) // use-time, in an Activity
}
```

The rule is identical in all three SDKs: **load config once at startup in process
code**, and **read the secret at use-time inside an Activity** — never in Workflow
code, where the non-deterministic read would break replay.

</details>

## Tasks

1. Build an `SsmClient` configured for LocalStack (same endpoint override as Lab 1).
2. `getParametersByPath("/temporal-training/worker/")` with `withDecryption=true`,
   following pagination, and map the tree into the `WorkerConfig` record.
3. Build the `WorkflowServiceStubs` / `WorkerFactory` from those values and confirm
   the Worker boots against `temporal-address` on `task-queue`.
4. Implement `fetchApiKey` and call it from a Workflow step that needs the key;
   confirm the SecureString comes back decrypted (not ciphertext).

## Verification

```bash
# The whole tree, with the SecureString decrypted:
awslocal ssm get-parameters-by-path \
  --path /temporal-training/worker/ --with-decryption
```

Expected: the four params print, and `api-key` shows its plaintext value (not an
encrypted blob). The Worker logs show it booted from the SSM values — task queue
`transform`, target `127.0.0.1:7233`. In the Temporal Web UI, a Workflow that
needs the key triggers the `fetchApiKey` Activity, visible as its own
`ActivityTaskCompleted`.

## Definition of done

- [ ] Config is read from SSM **at startup**, in process code — not in a Workflow.
- [ ] `getParametersByPath` loads the whole tree with decryption and pagination.
- [ ] The Worker builds its stubs / factory from the loaded `WorkerConfig`.
- [ ] The API key is read **inside the `fetchApiKey` Activity** when a step needs
      it, returns decrypted, and is never logged.

## Pitfalls

- **NEVER read SSM inside Workflow code.** It's non-deterministic I/O, and the
  parameter value can change between the original run and a replay — that breaks
  determinism. Config goes in startup code; per-run secrets go in an Activity.
- **Don't log decrypted secrets.** Once `WithDecryption` returns plaintext, keep
  it out of logs, Workflow history, and Activity *inputs*. Pass it straight into
  the downstream call.
- **Cache config at boot — don't hammer SSM per task.** Read the tree once at
  startup into an immutable snapshot; re-reading on every task adds latency and
  burns the Parameter Store throttle limit.

## Hints

<details><summary>Hint 1 — LocalStack SsmClient</summary>

Same builder shape as Lab 1's `GlueClient`:

```java
SsmClient.builder()
    .endpointOverride(URI.create("http://127.0.0.1:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```

In ECS/EKS, drop the endpoint override and credentials provider — the default
chain picks up the task role / IRSA identity automatically.
</details>

<details><summary>Hint 2 — getParametersByPath pagination + decryption</summary>

`getParametersByPath` returns at most ~10 params per page, so loop on
`NextToken` until it's null. Set `recursive(true)` so nested keys come back and
`withDecryption(true)` so `SecureString` values arrive as plaintext:

```java
GetParametersByPathRequest.builder()
    .path("/temporal-training/worker/")
    .recursive(true)
    .withDecryption(true)
    .nextToken(nextToken)   // null on the first call
    .build();
```
</details>

## Stretch goals

- React to a **parameter change** (e.g. a new `task-queue` value) by triggering a
  rolling restart of the Worker fleet — config changes shouldn't need a rebuild,
  but they do need a restart since the snapshot is cached at boot.
- Back the `SecureString` with a **dedicated KMS key alias** instead of the
  default `aws/ssm` key, and confirm `--with-decryption` still resolves it (in
  ECS/EKS the task role would need `kms:Decrypt` on that key).
