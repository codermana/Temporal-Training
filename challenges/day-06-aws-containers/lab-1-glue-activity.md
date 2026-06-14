# Lab 6.1 — Wrap a Glue job as an Activity

**Time:** ~50 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

A long-running Glue ETL job needs to be orchestrated reliably: started, polled to
completion, and have its failures surfaced where you can see them. In the AWS
world this is Step Functions + CloudWatch glue. Here it's a single Activity that
submits the Glue run, **heartbeats** while polling, and throws a structured
`ApplicationFailure` on a bad terminal state. LocalStack stands in for Glue.

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

Create a Glue job in LocalStack to target (a stub script is fine; LocalStack
will report run states). Use `awslocal glue create-job ...`. If your LocalStack
build doesn't fully simulate Glue run transitions, treat the polling loop as the
deliverable and simulate states as needed.

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

**Activity impl — complete the submit/poll loop:**

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

## Tasks

1. Build a `GlueClient` configured for LocalStack.
2. Implement submit → heartbeat → poll → terminal-state handling.
3. Call the Activity from a small Workflow (or reuse `ImportWorkflow` later) with
   a generous `startToCloseTimeout` (> worst-case job time) and a
   `heartbeatTimeout`.
4. Run a job to SUCCEEDED and one to FAILED; confirm the failure surfaces in the
   Temporal UI with your `GlueJobFailed` type and message.

## Verification

```bash
awslocal glue get-job-runs --job-name <your-job>     # see the runs Temporal started
```

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

- **`Thread.sleep` is fine here** — this is Activity code, not Workflow code. The
  determinism rules apply to Workflows, not Activities.
- **Heartbeat or die.** Without heartbeats, a Worker restart loses the job-run
  context and Temporal can't tell a stuck job from a slow one. Heartbeat the
  `jobRunId` so a resumed Activity can re-attach (`getHeartbeatDetails`).
- **Timeout alignment.** If `startToCloseTimeout` is shorter than the job,
  Temporal kills the Activity mid-job and retries — submitting the job twice.

## Hints

<details><summary>Hint 1 — LocalStack GlueClient</summary>

```java
GlueClient.builder()
    .endpointOverride(URI.create("http://127.0.0.1:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```
</details>

<details><summary>Hint 2 — resume after restart</summary>

On entry, check `Activity.getExecutionContext().getHeartbeatDetails(String.class)`
— if present, a previous attempt already started a run; re-attach to that
`jobRunId` instead of starting a new one.
</details>

## Stretch goals

- Implement the resume-after-restart path and test it by killing the Worker
  mid-poll.
- Add `--arguments` beyond `--input` and surface Glue's `errorMessage` verbatim
  in the `ApplicationFailure` details.
