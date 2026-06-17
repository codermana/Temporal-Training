# Lab 6.6: SQS event trigger → signalWithStart

**Time:** ~45 min · **Difficulty:** ★★ · **Stack:** Temporal + LocalStack

## Scenario

A file lands in S3 and something has to kick off an import. In AWS this is an
**EventBridge rule → Lambda → StartExecution** chain. Here you replace it with a
single long-poll **SQS consumer**, a plain bridge process that runs *outside*
any Workflow. It receives a file-arrival message and does **signalWithStart** on
an `ImportWorkflow`. Because `signalWithStart` is idempotent on the Workflow ID,
SQS's at-least-once redelivery just re-signals the *same* Workflow instead of
spawning a duplicate run. LocalStack provides the SQS queue.

> "Standard queues ensure at-least-once message delivery, but due to the highly distributed architecture, more than one copy of a message might be delivered, and messages may occasionally arrive out of order."
>
> *Amazon SQS Developer Guide*, docs.aws.amazon.com

<!-- source: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues.html -->

## Coming from EventBridge → Lambda → StartExecution

> | AWS | Here |
> |---|---|
> | EventBridge rule matches an S3 event | SQS message lands on `imports-events` |
> | Lambda parses the event | bridge parses the message body |
> | `StartExecution` (and pray it's the first one) | `signalWithStart` (idempotent on Workflow ID) |
> | Lambda returns → event is "done" | delete the SQS message **after** the signal is durable |
>
> The bridge is glue, not a Workflow or Activity: no determinism constraints,
> so plain loops, `boto3`, blocking SDK calls are all fine.

## Learning goals

- Trigger a Workflow from an external event source without putting the consumer
  *inside* a Workflow.
- Use `signalWithStart` for idempotency: at-least-once redelivery re-signals one
  Workflow rather than starting duplicates.
- Order the delete **after** the signal is durable in Temporal: at-least-once,
  never at-most-once.
- Long-poll SQS (`WaitTimeSeconds=20`) instead of hot-spinning.

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
# LocalStack S3/SQS/Glue on :4566 (SQS is supported on the free tier)
```

</details>

Create the queue the bridge will drain:

```bash
awslocal sqs create-queue --queue-name imports-events
# note the QueueUrl it prints; you'll pass it to the bridge and to send-message
```

This lab signals the `ImportWorkflow` from Labs 6.1–6.2. A Worker must be running
on the `transform` Task Queue so the started Workflow has somewhere to run.

## Starter code

Extend the `training.temporal.aws` module. The bridge is an ordinary `main`/loop,
**not** a Worker registration. `pom.xml` adds AWS SDK v2 SQS:

```xml
<dependency>
  <groupId>software.amazon.awssdk</groupId>
  <artifactId>sqs</artifactId>
  <version>2.25.0</version>
</dependency>
```

**Given contract**: a long-poll pump that signal-with-starts one Workflow per
message:

```java
// SqsSignalBridge.java: runs outside any Workflow (plain glue code).
class SqsSignalBridge {
  private final WorkflowClient client;
  private final SqsClient sqs;
  private final String queueUrl;

  void pump() {
    while (true) {
      // TODO 1: receiveMessage with maxNumberOfMessages(10) and
      //         waitTimeSeconds(20): long poll, NOT a hot spin.
      ReceiveMessageResponse resp = /* ... */;

      for (Message m : resp.messages()) {
        // TODO 2: parse(m.body()) -> bucket, key, s3Uri.
        FileEvent event = parse(m.body());

        // TODO 3: signalWithStart on workflowId "import-<bucket>-<key>".
        //   - newWorkflowStub(ImportWorkflow.class, options with that id +
        //     setTaskQueue("transform"))
        //   - newSignalWithStartRequest(); batch.add(stub::run, event.s3Uri())
        //   - client.signalWithStart(batch)
        //   signalWithStart starts the Workflow if absent, signals it if running.
        //   Idempotent on the Workflow ID: redelivery re-signals the same run.

        // TODO 4: deleteMessage(receiptHandle): ONLY after the signal returns.
        //   The signal is durable in Temporal before you delete -> at-least-once.
      }
    }
  }
}
```

Point the `SqsClient` at LocalStack: override the endpoint to
`http://127.0.0.1:4566`, region `us-east-1`, dummy credentials (same shape as the
`GlueClient`/`S3Client` in Labs 6.1–6.2).

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

Reference ports: [`examples/07-aws-containers/python/sqs_signal_bridge.py`](../../examples/07-aws-containers/python/sqs_signal_bridge.py)
and [`.../go/sqs_signal_bridge.go`](../../examples/07-aws-containers/go/sqs_signal_bridge.go).
Try the TODOs yourself before peeking. (`boto3` / `aws-sdk-go-v2` may be absent
offline; the Temporal-side `signalWithStart` + delete-after-durable ordering is
the deliverable.)

**Python** (`temporalio`), plain async loop, lazy `boto3`; `start_signal` makes
`start_workflow` behave as signal-with-start:

```python
import json
from temporalio.client import Client

def _sqs_client():
    import boto3  # lazy: AWS SDK may be absent offline
    return boto3.client("sqs")

async def pump(client: Client, queue_url: str) -> None:
    sqs = _sqs_client()
    while True:
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # long poll, not a hot spin
        )
        for m in resp.get("Messages", []):
            event = json.loads(m["Body"])
            s3_uri = event["s3Uri"]
            # start_signal: starts the Workflow if absent, signals it if running.
            await client.start_workflow(
                "ImportWorkflow", s3_uri,
                id=f"import-{event['bucket']}-{event['key']}",
                task_queue="transform",
                start_signal="file_arrived", start_signal_args=[s3_uri],
            )
            # Delete only after the signal is durable in Temporal: at-least-once.
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
```

**Go** (`go.temporal.io/sdk`, `aws-sdk-go-v2`), `SignalWithStartWorkflow`, then
delete:

```go
func Pump(ctx context.Context, c client.Client, sqs SqsAPI, queueURL string) error {
    for {
        msgs, err := sqs.Receive(ctx, queueURL) // long poll inside the impl
        if err != nil {
            return err
        }
        for _, m := range msgs {
            var ev fileEvent
            if err := json.Unmarshal([]byte(m.Body), &ev); err != nil {
                return err
            }
            // SignalWithStart: starts the Workflow if absent, signals it if running.
            _, err := c.SignalWithStartWorkflow(
                ctx,
                "import-"+ev.Bucket+"-"+ev.Key,
                "file_arrived", ev.S3URI,
                client.StartWorkflowOptions{TaskQueue: "transform"},
                "ImportWorkflow", ev.S3URI,
            )
            if err != nil {
                return err
            }
            // Delete only after the signal is durable in Temporal: at-least-once.
            if err := sqs.Delete(ctx, queueURL, m.ReceiptHandle); err != nil {
                return err
            }
        }
    }
}
```

The rule is identical in all three SDKs: **signal-with-start on a deterministic
Workflow ID** (`import-<bucket>-<key>`) so redelivery is harmless, and **delete
the message only after the signal returns** so a crash mid-flight redelivers
rather than drops.

</details>

## Tasks

1. Build an `SqsClient` configured for LocalStack and create `imports-events`.
2. Implement the pump: receive (long poll) → parse → `signalWithStart` → delete.
3. Derive the Workflow ID from the event (`import-<bucket>-<key>`) so the same
   file always maps to the same Workflow.
4. Run the bridge alongside a Worker on the `transform` Task Queue.

## Verification

```bash
# Capture the queue URL once:
QURL=$(awslocal sqs get-queue-url --queue-name imports-events --query QueueUrl --output text)

# Seed one file-arrival event:
awslocal sqs send-message --queue-url "$QURL" \
  --message-body '{"bucket":"imports-incoming","key":"orders.csv","s3Uri":"s3://imports-incoming/incoming/orders.csv"}'
```

In the Temporal Web UI: exactly **one** `ImportWorkflow` with ID
`import-imports-incoming-orders.csv` started (and received the `file_arrived`
signal). Now send the **same** message again:

```bash
awslocal sqs send-message --queue-url "$QURL" \
  --message-body '{"bucket":"imports-incoming","key":"orders.csv","s3Uri":"s3://imports-incoming/incoming/orders.csv"}'
```

Confirm **no second run** appears: the same Workflow ID is re-signalled, not
restarted. That is the idempotency guarantee `signalWithStart` buys you.

## Definition of done

- [ ] The bridge long-polls SQS (`WaitTimeSeconds=20`) and runs outside any
      Workflow/Activity.
- [ ] Each message does `signalWithStart` on a Workflow ID derived from the event.
- [ ] The SQS delete happens **only after** the signal returns (durable).
- [ ] Re-sending the same message produces **no** second Workflow run: one run,
      re-signalled.

## Pitfalls

- **Delete-before-durable loses events.** If you delete the message *before*
  `signalWithStart` returns, a crash in between drops the event entirely. Signal
  first, delete second: at-least-once.
- **Don't make it at-most-once.** Acking/deleting on receive (or auto-delete)
  trades a duplicate (which Temporal de-dupes) for a *lost* import (which it
  can't recover). At-least-once + idempotent ID is the safe combination.
- **The bridge is not a Workflow.** Don't put the receive loop inside a Workflow
  or Activity; there are no determinism constraints on glue code, and blocking
  SDK calls / `WaitTimeSeconds` belong here, not in Workflow code.
- **Don't hot-spin.** Omitting `WaitTimeSeconds` makes SQS return immediately and
  hammers the API. Long poll.

## Hints

<details><summary>Hint 1: LocalStack SqsClient</summary>

```java
SqsClient.builder()
    .endpointOverride(URI.create("http://127.0.0.1:4566"))
    .region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(
        AwsBasicCredentials.create("test", "test")))
    .build();
```
</details>

<details><summary>Hint 2: why the ID makes it idempotent</summary>

`signalWithStart` keys on the Workflow ID. Two messages for the same file both
resolve to `import-imports-incoming-orders.csv`: the first *starts* the Workflow,
the second finds it already running and only *signals* it. Make the ID a pure
function of the event (bucket + key) and redelivery is automatically harmless:
no de-dup table needed.
</details>

## Stretch goals

- Wire a **dead-letter queue**: set a redrive policy on `imports-events` so a
  message that fails parsing N times lands on `imports-events-dlq`, and drain the
  DLQ into a compensation Workflow that records the bad event.
- Handle a **full batch of 10**: `maxNumberOfMessages(10)` already pulls a batch,
  signal each, and delete them with a single `deleteMessageBatch` only after all
  their signals are durable.
</content>
</invoke>
