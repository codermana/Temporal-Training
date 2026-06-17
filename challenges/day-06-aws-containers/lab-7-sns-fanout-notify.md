# Lab 6.7: SNS fan-out notify Activity

**Time:** ~40 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

The `ImportWorkflow` from Labs 6.1–6.3 ends with a `notify` step. In AWS that
last step is a **Lambda that publishes to SNS**, fanning the completion event out
to email, SQS, and other Lambda subscribers. Here you move only the *publisher*
into Temporal: a `publishNotification` Activity publishes the import result to an
SNS topic, and **N decoupled subscribers stay exactly as they were**. You'll
subscribe an SQS queue to the topic so you can read the fan-out and prove it
still works. SNS (and SNS→SQS) are supported on the LocalStack free tier.

> "The Fanout scenario is when a message published to an SNS topic is replicated and pushed to multiple endpoints, such as Firehose delivery streams, Amazon SQS queues, HTTP(S) endpoints, and Lambda functions."
>
> *Amazon SNS Developer Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/sns/latest/dg/welcome.html -->

## Coming from SNS-publish-in-a-Lambda

> | AWS | Here |
> |---|---|
> | Lambda's last action: `sns.publish(...)` | an Activity publishes; the Workflow sequences it |
> | EventBridge / SNS fan-out to subscribers | **unchanged**, SNS still fans out to every subscription |
> | Lambda retried → maybe double-publish | Activity retried → maybe double-publish (same hazard) |
> | each subscriber dedups on its own | each subscriber dedups on `workflowId` in the message |
>
> Only the publisher moved into Temporal. The topic, the subscriptions, and the
> at-least-once contract are all the same; subscribers don't know or care that
> the publish now comes from an Activity instead of a Lambda.

## Learning goals

- Notifications are I/O, so publishing one is an **Activity**, never Workflow code.
- SNS delivery is **at-least-once**: a retried Activity may double-publish, so the
  message carries `workflowId` (+ `runId`) and subscribers dedup on it.
- SNS→SQS fan-out is unchanged by the migration; only the publisher moved.
- Keep the message small: a notification carries a *reference* (the output URI),
  not the imported data.

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
# LocalStack S3/SQS/SNS/Glue on :4566 (SNS + SNS→SQS work on the free tier)
```

</details>

Create the topic, a subscriber queue, and wire them together so the lab can
verify fan-out by draining the queue:

```bash
# The topic the Activity will publish to:
TOPIC=$(awslocal sns create-topic --name imports-complete --query TopicArn --output text)

# One decoupled subscriber: an SQS queue you can inspect.
QURL=$(awslocal sqs create-queue --queue-name imports-notify-sub --query QueueUrl --output text)
QARN=$(awslocal sqs get-queue-attributes --queue-url "$QURL" \
        --attribute-names QueueArn --query Attributes.QueueArn --output text)

# Subscribe the queue to the topic (RawMessageDelivery keeps the body as your JSON):
awslocal sns subscribe --topic-arn "$TOPIC" --protocol sqs --notification-endpoint "$QARN" \
  --attributes RawMessageDelivery=true
```

This lab extends the `ImportWorkflow` from Labs 6.1–6.3. A Worker must be running
on the import Task Queue so the Workflow (and its new Activity) has somewhere to
run.

## Starter code

Extend the `training.temporal.aws` module. `pom.xml` adds AWS SDK v2 SNS:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>sns</artifactId>
  <version>2.25.0</version>
</dependency>
```

**Given contract**: the notify step as an Activity that returns the SNS message id:

```java
// SnsPublishActivities.java
@ActivityInterface
public interface SnsPublishActivities {
  @ActivityMethod
  String publishNotification(String workflowId, long rowCount, String outputS3Uri); // -> SNS messageId
}
```

**Activity impl, complete the publish:**

```java
public class SnsPublishActivitiesImpl implements SnsPublishActivities {
  private final SnsClient sns;     // TODO: build for LocalStack (see Hint 1)
  private final String topicArn;   // the imports-complete topic ARN

  @Override
  public String publishNotification(String workflowId, long rowCount, String outputS3Uri) {
    // TODO 1: build a SMALL JSON message: workflowId, the runId
    //   (Activity.getExecutionContext().getInfo().getRunId()), rowCount, outputS3Uri.
    //   The workflowId is what subscribers dedup on: at-least-once delivery.
    // TODO 2: sns.publish(PublishRequest ... topicArn(topicArn).message(json)
    //   .messageDeduplicationId(workflowId)).  On a FIFO topic that dedup id makes
    //   SNS collapse a retried publish; harmless on a standard topic.
    // TODO 3: return resp.messageId() so it shows up as the Activity result in the UI.
    throw new UnsupportedOperationException("TODO");
  }
}
```

Point the `SnsClient` at LocalStack: override the endpoint to
`http://127.0.0.1:4566`, region `us-east-1`, dummy credentials (same shape as the
`GlueClient`/`S3Client`/`SqsClient` in Labs 6.1–6.6).

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference ports: [`examples/07-aws-containers/python/sns_publish_activity.py`](../../examples/07-aws-containers/python/sns_publish_activity.py)
and [`.../go/sns_publish_activity.go`](../../examples/07-aws-containers/go/sns_publish_activity.go).
Try the TODOs yourself before peeking. (`boto3` / `aws-sdk-go-v2` may be absent
offline; the Temporal-side "publish-as-an-Activity, include workflowId for
dedup" pattern is the deliverable.)

**Python** (`temporalio`), module-level Activity, lazy `boto3`, client pointed at
LocalStack:

```python
import json
from temporalio import activity

def _sns_client():
    import boto3  # lazy: AWS SDK may be absent offline
    return boto3.client(
        "sns", endpoint_url="http://127.0.0.1:4566", region_name="us-east-1",
        aws_access_key_id="test", aws_secret_access_key="test",
    )

@activity.defn
async def publish_notification(topic_arn: str, workflow_id: str, row_count: int, output_s3_uri: str) -> str:
    sns = _sns_client()
    message = json.dumps({
        "workflowId": workflow_id,
        "runId": activity.info().workflow_run_id,  # subscribers dedup on workflowId
        "rowCount": row_count,
        "outputS3Uri": output_s3_uri,              # a reference, not the data
    })
    resp = sns.publish(
        TopicArn=topic_arn, Subject="import-complete", Message=message,
        MessageDeduplicationId=workflow_id,  # FIFO collapses a retried publish
    )
    return resp["MessageId"]
```

**Go** (`go.temporal.io/sdk`, `aws-sdk-go-v2`), publish behind a small `SnsAPI`,
return the message id:

```go
func (a *SnsActivities) PublishNotification(ctx context.Context, topicARN, workflowID string, rowCount int64, outputS3URI string) (string, error) {
    body, err := json.Marshal(importNotice{
        WorkflowID:  workflowID,
        RunID:       activity.GetInfo(ctx).WorkflowExecution.RunID, // subscribers dedup on workflowId
        RowCount:    rowCount,
        OutputS3URI: outputS3URI,                                   // a reference, not the data
    })
    if err != nil { return "", err }
    // dedupID = workflowID: a FIFO topic collapses a retried publish into one delivery.
    return a.Sns.Publish(ctx, topicARN, "import-complete", string(body), workflowID)
}
```

The rule is identical in all three SDKs: **publish from an Activity** (not the
Workflow), **put the `workflowId` in the message** so at-least-once delivery is
safe to dedup, and **return the SNS message id** so the publish is visible in the
Web UI.

</details>

## Tasks

1. Build an `SnsClient` configured for LocalStack and create `imports-complete`.
2. Implement `publishNotification`: small JSON (`workflowId`, `runId`, `rowCount`,
   `outputS3Uri`) → `sns.publish` → return the message id.
3. Wire the Activity as the **last step** of the import pipeline
   (`validate → transform → load → publishNotification`), passing the
   `Workflow.getInfo().getWorkflowId()` so subscribers can dedup.
4. Run an import end-to-end and read the subscribed SQS queue to see the fan-out.

## Verification

```bash
# Drain the subscribed queue: the notification fanned out from SNS to SQS:
awslocal sqs receive-message --queue-url "$QURL" --wait-time-seconds 5
```

Expected: one message whose body is your JSON, containing the `workflowId`,
`rowCount`, and `outputS3Uri`. In the Temporal Web UI: the `publishNotification`
Activity completes with the SNS `messageId` as its result, as the final event in
the import.

## Definition of done

- [ ] `publishNotification` is an **Activity**, called as the last step of the
      Workflow; no SNS publish happens in Workflow code.
- [ ] The published message includes the `workflowId` (and `runId`) so an
      at-least-once redelivery is safe to dedup.
- [ ] The subscribed SQS queue receives the notification: SNS→SQS fan-out works
      unchanged.
- [ ] The Activity returns the SNS `messageId`, visible in the Web UI.

## Pitfalls

- **Don't publish from Workflow code.** `sns.publish` is non-deterministic I/O;
  it belongs in an Activity. The Workflow only *sequences* the call.
- **Publish is at-least-once.** A retried or timed-out Activity may publish the
  same notification twice, so SNS may deliver duplicates. The `workflowId` in the
  body is the dedup key for subscribers (or use a FIFO topic with the `workflowId`
  as the message-deduplication id). Never assume exactly-once delivery.
- **Keep the message small, it's a notification, not a data bus.** Send the
  output **URI**, never the imported rows. Workflow history (and the SNS message)
  has a payload-size limit; keep individual payloads well under ~2 MB (see Lab
  6.2). Put the data in S3; send a reference.

## Hints

<details><summary>Hint 1: LocalStack SnsClient</summary>

```java
SnsClient.builder()
    .endpointOverride(URI.create("http://127.0.0.1:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```
</details>

<details><summary>Hint 2: why the workflowId makes it safe to retry</summary>

The Activity is at-least-once, so the same notification can be published twice.
Putting `workflowId` (a stable, deterministic id) in every message lets each
subscriber keep a "seen" set keyed on it and drop the duplicate. On a **FIFO**
topic you go one better: pass `workflowId` as `MessageDeduplicationId` and SNS
itself collapses the retried publish within its dedup window; no subscriber-side
table needed.
</details>

## Stretch goals

- Switch `imports-complete` to a **FIFO topic** (`.fifo` name) and a FIFO SQS
  subscriber; pass the `workflowId` as the `MessageDeduplicationId` and confirm a
  forced double-publish (retry the Activity) lands **once** on the queue.
- Make the SQS subscriber a **chain**: have it `signalWithStart` *another*
  Workflow (e.g. a downstream "reconcile" import), reusing the SQS→signal bridge
  from Lab 6.6, so SNS fan-out triggers the next Workflow with no Lambda glue.
