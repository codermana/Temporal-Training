# Lab 6.3 — Migrate a Step Functions pipeline

**Time:** ~60 min · **Difficulty:** ★★★ · **Stack:** Temporal + LocalStack

## Scenario

You're handed a Step Functions state machine (JSON): `validate → transform →
load → notify`, with `Retry` and `Catch` blocks and intermediate state passed via
S3. Rewrite it as **one Temporal Workflow + four Activities** in Java, then
compare the two on lines of code, error surfaces, retry config, and
debuggability. Finally, plan a strangler-fig cutover.

## Learning goals

- Translate a Step Functions state machine into a Workflow + Activities.
- Replace `Retry`/`Catch` JSON with `RetryOptions` and try/catch.
- Compare debuggability: one Event History vs. CloudWatch + S3 + execution graph.
- Plan a parallel-run (strangler fig) migration with a feature flag.

## Reference Step Functions definition

Use this as the "before" artifact (a trimmed, representative state machine):

```json
{
  "Comment": "Daily import pipeline",
  "StartAt": "Validate",
  "States": {
    "Validate": {
      "Type": "Task", "Resource": "arn:aws:lambda:...:validate",
      "Retry": [{ "ErrorEquals": ["States.TaskFailed"], "MaxAttempts": 3, "IntervalSeconds": 2, "BackoffRate": 2.0 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "Transform"
    },
    "Transform": {
      "Type": "Task", "Resource": "arn:aws:glue:...:transform",
      "Retry": [{ "ErrorEquals": ["States.ALL"], "MaxAttempts": 3 }],
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "Load"
    },
    "Load": {
      "Type": "Task", "Resource": "arn:aws:lambda:...:load",
      "Catch": [{ "ErrorEquals": ["States.ALL"], "Next": "NotifyFailure" }],
      "Next": "NotifySuccess"
    },
    "NotifySuccess": { "Type": "Task", "Resource": "arn:aws:sns:...:notify", "End": true },
    "NotifyFailure": { "Type": "Task", "Resource": "arn:aws:sns:...:notify", "End": true }
  }
}
```

## Starter code

Extend `training.temporal.aws`. **Given contract:**

```java
// ImportWorkflow.java
@WorkflowInterface
public interface ImportWorkflow {
  @WorkflowMethod
  String run(String inputS3Uri);   // returns the final transformed URI + row count
}
```

Reuse `ImportActivities` from Lab 6.2 and add a `notify(...)` Activity (it can log
or write to an SNS/SQS mock in LocalStack).

**Workflow impl — translate the state machine:**

```java
public class ImportWorkflowImpl implements ImportWorkflow {
  private final ImportActivities activities =
      Workflow.newActivityStub(
          ImportActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofMinutes(2))
              // Step Functions Retry -> RetryOptions
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(2))
                      .setBackoffCoefficient(2.0)
                      .setMaximumAttempts(3)
                      .build())
              .build());

  @Override
  public String run(String inputS3Uri) {
    // TODO: validate -> transform -> load, then notify success.
    //       The Step Functions Catch(NotifyFailure) becomes a try/catch that
    //       calls notify(...) with the failure and rethrows or returns a failure.
    throw new UnsupportedOperationException("TODO");
  }
}
```

## Tasks

1. Map each state → Activity call; map `Retry` → `RetryOptions`; map `Catch` →
   try/catch calling `notify`.
2. Implement `run` so the happy path notifies success and any failure notifies
   failure (your "NotifyFailure" branch).
3. Run happy and failing inputs against LocalStack.
4. **Write the comparison** (a short paragraph): LOC, where errors show up, how
   you'd debug a stuck run in each system.

## Verification

```bash
make run-aws        # the Import Worker (needs stack-aws + temporal)

# start a run
make start-workflow QUEUE=transform ID=1 TYPE=ImportWorkflow \
  INPUT="s3://imports-incoming/incoming/orders.csv"
```

<details><summary>Under the hood — what <code>make run-aws</code> runs</summary>

```bash
cd examples/runnable/08-aws-containers && mvn -q compile exec:java \
  -Dexec.mainClass=training.temporal.aws.WorkerMain
```

</details>

<details><summary>Under the hood — what <code>make start-workflow</code> runs</summary>

```bash
temporal workflow start \
  --task-queue transform \
  --type ImportWorkflow \
  --workflow-id importworkflow-1 \
  --input "\"s3://imports-incoming/incoming/orders.csv\""
# TYPE defaults to ImportWorkflow; workflow-id is <type-lowercased>-<ID>;
# INPUT defaults to s3://imports-incoming/synthetic-<ID>.csv.
```

</details>

Expected: the Workflow runs all four steps; a failing input routes through your
NotifyFailure path. Everything — inputs, retries, the failure, the notify — is in
**one** Event History.

## Definition of done

- [ ] All four states are Activities orchestrated by one Workflow.
- [ ] `Retry`/`Catch` semantics are reproduced with `RetryOptions` + try/catch.
- [ ] Happy and failure paths both notify correctly.
- [ ] You can articulate the debuggability difference (one history vs.
      CloudWatch + S3 + Step Functions graph) and a strangler-fig cutover plan.

## Pitfalls

- **Don't over-translate.** Step Functions needs explicit `Pass`/`Choice` states
  for branching; in a Workflow that's just an `if`. Resist recreating the state
  machine shape — write straight-line Java.
- `BackoffRate` → `setBackoffCoefficient`; `IntervalSeconds` →
  `setInitialInterval`; `MaxAttempts` → `setMaximumAttempts`.

## Strangler-fig migration (discuss / sketch)

- Run Step Functions and the Temporal Workflow **in parallel** behind a feature
  flag; route a small % of inputs to Temporal.
- Compare outputs for the same input (shadow mode) before cutting traffic over.
- Decommission the state machine only once Temporal handles 100% with parity.

## Stretch goals

- Add a real `Choice`-style branch (e.g. skip transform for already-clean input)
  and show how trivial it is in code.
- Add a child Workflow for the transform stage and discuss when splitting a
  state machine into parent/child Workflows is worth it.
