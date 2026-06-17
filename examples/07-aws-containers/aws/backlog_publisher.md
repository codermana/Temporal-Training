# Backlog publisher — the metric KEDA gives you for free

KEDA's native Temporal scaler polls `DescribeTaskQueue` for you and feeds the
backlog straight into a Kubernetes HPA. **ECS has no equivalent scaler.** So you
publish the signal yourself: a tiny loop calls `DescribeTaskQueue`, computes the
backlog, and writes it to CloudWatch as a custom metric. Application Auto Scaling
(see `ecs_autoscaling.json`) then target-tracks on that metric — the CloudWatch
analog of KEDA's `targetQueueSize`.

**Where it runs.** Either is fine:

- A **sidecar container** in the same task definition as the Worker (one publisher
  per task — cheap, co-located, scales with the service). Or
- A **single scheduled task** / standalone service that publishes once for the
  whole queue (one publisher total — simpler metric, but its own thing to keep
  alive). Prefer this when you don't want N publishers all writing the same point.

The metric namespace, name, and dimensions below must match `ecs_autoscaling.json`
exactly (`Temporal/Worker` / `TaskQueueBacklog` / `TaskQueue=transform`).

## The loop (illustrative Java — AWS SDK v2 + Temporal SDK)

```java
// Runs forever; one DescribeTaskQueue + one PutMetricData per tick (~15s).
WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(/* address, TLS, api-key */);
CloudWatchClient cw = CloudWatchClient.create();
String namespace = System.getenv("TEMPORAL_NAMESPACE");
String taskQueue = System.getenv().getOrDefault("TASK_QUEUE", "transform");

while (true) {
  DescribeTaskQueueResponse resp = service.blockingStub().describeTaskQueue(
      DescribeTaskQueueRequest.newBuilder()
          .setNamespace(namespace)
          .setTaskQueue(TaskQueue.newBuilder().setName(taskQueue).build())
          .setApiMode(DescribeTaskQueueMode.DESCRIBE_TASK_QUEUE_MODE_ENHANCED)  // gives backlog stats
          .addTaskQueueTypes(TASK_QUEUE_TYPE_ACTIVITY)                          // match your worker pool
          .build());

  long backlog = resp.getVersionsInfoMap().values().stream()
      .flatMap(v -> v.getTypesInfoMap().values().stream())
      .mapToLong(t -> t.getStats().getApproximateBacklogCount())               // depth waiting to be polled
      .sum();

  cw.putMetricData(PutMetricDataRequest.builder()
      .namespace("Temporal/Worker")
      .metricData(MetricDatum.builder()
          .metricName("TaskQueueBacklog")
          .dimensions(Dimension.builder().name("TaskQueue").value(taskQueue).build())
          .unit(StandardUnit.COUNT)
          .value((double) backlog)
          .build())
      .build());

  Thread.sleep(15_000);   // poll cadence; CloudWatch resolves at 1-min granularity
}
```

> **IAM:** the publisher needs `cloudwatch:PutMetricData` on its **task role**
> (not the execution role — see the lab) plus whatever credential reaches your
> Temporal Frontend (an SSM-sourced API key for Temporal Cloud, or network reach
> to a self-hosted Frontend).

**Python / Go** are the same shape: `client.workflow_service.describe_task_queue(...)`
(or `DescribeTaskQueueEnhanced`) → sum `approximate_backlog_count` →
`boto3`/`aws-sdk-go-v2` `put_metric_data` into `Temporal/Worker` / `TaskQueueBacklog`.
