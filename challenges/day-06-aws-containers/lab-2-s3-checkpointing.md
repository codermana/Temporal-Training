# Lab 6.2 — Replace S3 checkpointing

**Time:** ~40 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

A classic AWS pipeline writes each step's output to S3 so the next Lambda can
pick it up and so a crash can resume from the last checkpoint. Temporal's durable
state makes the *checkpoint-for-resumption* part unnecessary — the Workflow
already survives crashes. You'll keep S3 for **large payloads** (passing URIs,
not bytes, between steps) but let Temporal own *whether each step ran*.

## Learning goals

- Distinguish "S3 as a crash checkpoint" (eliminated) from "S3 as a blob store"
  (kept, by reference).
- Pass S3 **URIs** as Activity inputs/outputs to keep Workflow history lean.
- Understand the payload-size limit and why blobs go in S3, references in history.

## Prerequisites

- Lab 6.1 patterns (LocalStack client config). `make temporal` + `make stack-aws`.
- Create a bucket: `awslocal s3 mb s3://imports-incoming` (and `validated`,
  `transformed` as you like, or use key prefixes in one bucket).

## Starter code

Extend the `training.temporal.aws` module. **Given contract** (the three import
steps; note they return/accept **URIs**, not data):

```java
// ImportActivities.java
@ActivityInterface
public interface ImportActivities {
  String validate(String inputS3Uri);      // -> validatedS3Uri
  String transform(String validatedS3Uri); // -> transformedS3Uri
  long   load(String transformedS3Uri);     // -> row count
}
```

**Activity impl — complete it.** Each step reads from one S3 URI and writes to
the next; it returns the *next* URI, never the file contents:

```java
public class ImportActivitiesImpl implements ImportActivities {
  // an S3Client pointed at LocalStack

  @Override
  public String validate(String inputS3Uri) {
    // TODO: read object at inputS3Uri, validate, write to a /validated/ key,
    //       heartbeat, and RETURN the new S3 URI (not the bytes).
    throw new UnsupportedOperationException("TODO");
  }
  // transform: /validated/ -> /transformed/ ; load: read transformed, return row count
}
```

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference ports: [`examples/07-aws-containers/python/s3_reference_payload.py`](../../examples/07-aws-containers/python/s3_reference_payload.py)
and [`.../go/s3_reference_payload.go`](../../examples/07-aws-containers/go/s3_reference_payload.go).
Each step returns the **next URI**, never the bytes.

**Python** (`temporalio`) — module-level Activities, lazy `boto3`:

```python
from temporalio import activity, workflow

@activity.defn
async def validate(input_s3_uri: str) -> str:
    s3 = boto3.client("s3")  # import boto3 lazily
    # read input, write to /validated/, heartbeat, RETURN the new URI (not the bytes)
    activity.heartbeat("validated")
    return input_s3_uri.replace("/incoming/", "/validated/")
# transform: /validated/ -> /transformed/ ; load: read transformed, return row count

@workflow.defn
class ImportWorkflow:
    @workflow.run
    async def run(self, input_s3_uri: str) -> str:
        from datetime import timedelta
        opts = dict(start_to_close_timeout=timedelta(minutes=2))
        validated = await workflow.execute_activity(validate, input_s3_uri, **opts)
        transformed = await workflow.execute_activity(transform, validated, **opts)
        rows = await workflow.execute_activity(load, transformed, **opts)
        return f"{transformed}?rows={rows}"
```

**Go** (`go.temporal.io/sdk`) — Activities return URIs; the Workflow sequences them:

```go
func Validate(ctx context.Context, inputS3URI string) (string, error) {
    activity.RecordHeartbeat(ctx, "validated")
    // read input, write to /validated/, RETURN the new URI (not the bytes)
    return strings.Replace(inputS3URI, "/incoming/", "/validated/", 1), nil
}

func ImportWorkflow(ctx workflow.Context, inputS3URI string) (string, error) {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 2 * time.Minute})
    var validated, transformed string
    var rows int64
    _ = workflow.ExecuteActivity(ctx, Validate, inputS3URI).Get(ctx, &validated)
    _ = workflow.ExecuteActivity(ctx, Transform, validated).Get(ctx, &transformed)
    _ = workflow.ExecuteActivity(ctx, Load, transformed).Get(ctx, &rows)
    return fmt.Sprintf("%s?rows=%d", transformed, rows), nil
}
```

The rule is identical in all three SDKs: **pass S3 references (URIs) between
steps, never file contents**, so Workflow history stays under the payload-size
limit.

</details>

## Tasks

1. Configure an `S3Client` for LocalStack (same endpoint override as Lab 6.1).
2. Implement `validate`/`transform`/`load` so each passes an S3 **URI** forward.
3. Drive them from a Workflow (sequential: `validate → transform → load`) and
   confirm the row count comes back.
4. Inspect the Workflow history: confirm only small URI strings appear in event
   payloads — **not** file contents.

## Verification

```bash
# Seed an input object
echo "id,amount
1,10
2,20" | awslocal s3 cp - s3://imports-incoming/incoming/orders.csv

# Start the import workflow (URI as input), then:
awslocal s3 ls s3://imports-incoming/validated/
awslocal s3 ls s3://imports-incoming/transformed/
```

Expected: validated and transformed objects exist; the Workflow returns the row
count. In the Web UI, Activity inputs/outputs are short URIs.

## Definition of done

- [ ] Each step passes an S3 URI forward; the bytes never enter Workflow history.
- [ ] The pipeline runs end-to-end against LocalStack S3 and returns a row count.
- [ ] You can explain which S3 use (checkpointing vs. blob storage) Temporal
      eliminates and which it keeps.

## Pitfalls

- **Don't return file contents from an Activity** if they can be large —
  Workflow history has a payload size limit (keep individual payloads well under
  ~2 MB; total history bounded too). Return the URI; let the next Activity fetch.
- The point isn't "stop using S3" — it's "stop using S3 as your durability
  mechanism." Temporal provides durability; S3 stays a blob store.

## Hints

<details><summary>Hint 1 — URI rewriting</summary>

A simple convention makes the pipeline obvious:
`s3://bucket/incoming/x.csv` → `.../validated/x.csv` → `.../transformed/x.csv`.
Each step `getObject` from its input prefix and `putObject` to the next.
</details>

<details><summary>Hint 2 — large payloads in general</summary>

For payloads that must live *in* history (not S3), look at a custom
`PayloadConverter` / codec. For this lab, references-in-history is the right and
simpler pattern.
</details>

## Stretch goals

- Add a **codec server** that encrypts the S3 URI payloads at rest in Temporal
  history, and confirm the UI shows ciphertext until the codec decodes it.
- Make `transform` idempotent on the output key so a retried Activity doesn't
  double-write.
