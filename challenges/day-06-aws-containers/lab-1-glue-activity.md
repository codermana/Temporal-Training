# Lab 6.1: Wrap a Glue job as an Activity

**Time:** ~50 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

A long-running ETL job needs to be orchestrated reliably: started, polled to
completion, and have its failures surfaced where you can see them. In the AWS
world this is Step Functions + CloudWatch glue. Here it's a single Activity that
submits the job run, **heartbeats** while polling, and throws a structured
`ApplicationFailure` on a bad terminal state. The job itself is a **self-hosted
local runner** doing a real S3 read→merge→write — no AWS Glue, no real-AWS calls
(see the note below).

## Learning goals

- Submit and poll an external long-running job from inside an Activity.
- Heartbeat the job-run id so Temporal knows the Activity is alive (and can
  resume polling after a Worker restart).
- Map terminal job states to success vs. `ApplicationFailure`.
- Align `startToCloseTimeout` with worst-case job duration.

## Prerequisites

```bash
make temporal      # terminal 1
make stack-aws     # terminal 2: LocalStack on :4566
```

<details><summary>Under the hood: what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
# gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
# Overridable via TEMPORAL_HOST, TEMPORAL_PORT, TEMPORAL_UI_PORT, TEMPORAL_METRICS_PORT.
```

</details>

<details><summary>Under the hood: what <code>make stack-aws</code> runs</summary>

```bash
docker compose -f docker/compose.localstack.yml up -d
# LocalStack S3/SQS/SNS/SSM on :4566
```

</details>

> **Note: we don't use AWS Glue — and we never call real AWS.** Glue is a
> **paid-tier emulator** on LocalStack (Ultimate), so the runnable supervises a
> **self-hosted local job runner** instead — a background job that does a *real*
> S3 read→merge→write against LocalStack (read the raw `.parquet` parts, merge
> them, write the curated object). The deliverable is the supervise-via-Activity
> pattern (start → poll + heartbeat → settle), and that pattern is **byte-for-byte
> identical** to a real `GlueClient`-backed runner — only the thing behind the
> seam changes. A complete, runnable version of exactly this is
> [`examples/runnable/17-spring-glue-pipeline`](../../examples/runnable/17-spring-glue-pipeline/)
> (`LocalStitchJobRunner`). The `GlueClient` code below is the **real-AWS target
> shape**: write it that way and it drops onto a real Glue job unchanged.

## Starter code

Module `training.temporal.aws`. `pom.xml` adds AWS SDK v2 Glue:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>glue</artifactId>
  <version>2.25.0</version>
</dependency>
```

**Given contract:**

```java
// GlueJobActivities.java
@ActivityInterface
public interface GlueJobActivities {
  @ActivityMethod
  String runGlueJob(String jobName, String inputS3Uri);   // returns the jobRunId
}
```

**Activity impl, complete the submit/poll loop:**

```java
public class GlueJobActivitiesImpl implements GlueJobActivities {
  private final GlueClient glue;
  public GlueJobActivitiesImpl(GlueClient glue) { this.glue = glue; }

  @Override
  public String runGlueJob(String jobName, String inputS3Uri) {
    // TODO 1: glue.startJobRun(...) with --input=inputS3Uri argument; capture jobRunId.
    // TODO 2: loop:
    //   - Activity.getExecutionContext().heartbeat(jobRunId)
    //   - glue.getJobRun(...).jobRun().jobRunState()
    //   - SUCCEEDED -> return jobRunId
    //   - FAILED/TIMEOUT/STOPPED -> throw ApplicationFailure.newFailure(msg, "GlueJobFailed")
    //   - else sleep ~15s and poll again (Thread.sleep is OK *inside an Activity*)
    throw new UnsupportedOperationException("TODO");
  }
}
```

Point the `GlueClient` at LocalStack: override the endpoint to
`http://127.0.0.1:4566` and region `us-east-1` with dummy credentials.

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference ports: [`examples/07-aws-containers/python/glue_activity.py`](../../examples/07-aws-containers/python/glue_activity.py)
and [`.../go/glue_activity.go`](../../examples/07-aws-containers/go/glue_activity.go).
Try the TODOs yourself before peeking. (`boto3` / `aws-sdk-go-v2` may be absent
offline; the Temporal-side submit/heartbeat/poll loop is the deliverable.)

**Python** (`temporalio`), module-level Activity, lazy `boto3`:

```python
import asyncio
from datetime import timedelta
from temporalio import activity
from temporalio.exceptions import ApplicationError

@activity.defn
async def run_glue_job(job_name: str, input_s3_uri: str) -> str:
    glue = boto3.client("glue")  # import boto3 lazily
    run_id = glue.start_job_run(JobName=job_name, Arguments={"--input": input_s3_uri})["JobRunId"]
    while True:
        activity.heartbeat(run_id)
        state = glue.get_job_run(JobName=job_name, RunId=run_id)["JobRun"]["JobRunState"]
        if state == "SUCCEEDED":
            return run_id
        if state in {"FAILED", "TIMEOUT", "STOPPED"}:
            raise ApplicationError(f"Glue job {job_name} ended as {state}", type="GlueJobFailed")
        await asyncio.sleep(timedelta(seconds=15).total_seconds())   # back off between polls
```

**Go** (`go.temporal.io/sdk`), heartbeat, then map terminal state to a typed failure:

```go
func (a *GlueActivities) RunGlueJob(ctx context.Context, jobName, inputS3URI string) (string, error) {
    runID, err := a.Glue.StartJobRun(ctx, jobName, inputS3URI) // wrap aws-sdk-go-v2 glue
    if err != nil { return "", err }
    for {
        activity.RecordHeartbeat(ctx, runID)
        state, errMsg, err := a.Glue.JobRunState(ctx, jobName, runID)
        if err != nil { return "", err }
        switch state {
        case "SUCCEEDED":
            return runID, nil
        case "FAILED", "TIMEOUT", "STOPPED":
            return "", temporal.NewApplicationError(
                fmt.Sprintf("Glue job %s ended as %s: %s", jobName, state, errMsg), "GlueJobFailed")
        }
        select {
        case <-ctx.Done(): return "", ctx.Err()
        case <-time.After(15 * time.Second):   // back off between polls
        }
    }
}
```

The rule is identical in all three SDKs: **heartbeat the run id every poll** so a
Worker restart can resume polling, and **map terminal states** to success vs. a
typed `ApplicationFailure`/`ApplicationError`.

</details>

## Tasks

1. Build a `GlueClient` configured for LocalStack.
2. Implement submit → heartbeat → poll → terminal-state handling.
3. Call the Activity from a small Workflow (or reuse `ImportWorkflow` later) with
   a generous `startToCloseTimeout` (> worst-case job time) and a
   `heartbeatTimeout`.
4. Run a job to SUCCEEDED and one to FAILED; confirm the failure surfaces in the
   Temporal UI with your `GlueJobFailed` type and message.

## Verification

Since the job is a self-hosted local runner (no Glue API), verify from the
Temporal side rather than `awslocal glue get-job-runs` (which errors — Glue is a
paid-tier emulator). The runner logs each `startJobRun`/`getJobRun` to the Worker
console; watch the poll loop there, then confirm the curated object really landed
in S3 with `awslocal s3 ls`.

In the Temporal Web UI: the successful run's Activity completes with the
`jobRunId`; the failed run shows an `ActivityTaskFailed` carrying the
`GlueJobFailed` type and the Glue error message.

## Definition of done

- [ ] The Activity submits a Glue run and returns its `jobRunId` on success.
- [ ] It heartbeats the `jobRunId` while polling.
- [ ] A failed/timeout/stopped job becomes an `ApplicationFailure` visible in the
      UI (not a generic stack trace).
- [ ] `startToCloseTimeout` is set to exceed the worst-case job duration; a
      `heartbeatTimeout` is set shorter.

## Pitfalls

- **`Thread.sleep` is fine here**: this is Activity code, not Workflow code. The
  determinism rules apply to Workflows, not Activities.
- **Heartbeat or die.** Without heartbeats, a Worker restart loses the job-run
  context and Temporal can't tell a stuck job from a slow one. Heartbeat the
  `jobRunId` so a resumed Activity can re-attach (`getHeartbeatDetails`).
- **Timeout alignment.** If `startToCloseTimeout` is shorter than the job,
  Temporal kills the Activity mid-job and retries, submitting the job twice.

## Hints

<details><summary>Hint 1: LocalStack GlueClient</summary>

```java
GlueClient.builder()
    .endpointOverride(URI.create("http://127.0.0.1:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```
</details>

<details><summary>Hint 2: resume after restart</summary>

On entry, check `Activity.getExecutionContext().getHeartbeatDetails(String.class)`
if present, a previous attempt already started a run; re-attach to that
`jobRunId` instead of starting a new one.
</details>

## Stretch goals

- Implement the resume-after-restart path and test it by killing the Worker
  mid-poll.
- Add `--arguments` beyond `--input` and surface Glue's `errorMessage` verbatim
  in the `ApplicationFailure` details.
