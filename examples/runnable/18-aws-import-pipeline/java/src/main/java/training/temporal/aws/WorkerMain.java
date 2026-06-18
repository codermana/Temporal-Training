package training.temporal.aws;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.ssm.SsmClient;

/**
 * The Worker entrypoint for the import pipeline. One process hosts three things:
 *
 * <ul>
 *   <li>the Temporal Worker on the import Task Queue (the pipeline itself),
 *   <li>the SQS trigger bridge (Lab 6.6), started when an events queue is configured,
 *   <li>a graceful-shutdown hook that drains in-flight work on {@code SIGTERM} (Lab 6.4).
 * </ul>
 *
 * <p>Its Temporal connection config is loaded from <b>SSM Parameter Store at
 * startup</b> (Lab 6.8) via {@link WorkerBootstrap}, falling back to env defaults
 * if SSM is unreachable. The AWS clients point at LocalStack by default (see
 * {@link AwsClients}); clearing {@code AWS_ENDPOINT} switches everything to real AWS.
 */
public final class WorkerMain {

  private WorkerMain() {}

  public static void main(String[] args) {
    // AWS clients (LocalStack by default).
    SsmClient ssm = AwsClients.ssm();
    S3Client s3 = AwsClients.s3();
    SnsClient sns = AwsClients.sns();
    SqsClient sqs = AwsClients.sqs();

    // Lab 6.8: load Temporal connection config from SSM at startup.
    WorkerConfig cfg = WorkerBootstrap.loadConfigOrDefaults(ssm);

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(cfg.temporalAddress()).build());
    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            WorkflowClientOptions.newBuilder().setNamespace(cfg.namespace()).build());

    // Pace each transform stage so the supervising Activity's heartbeats are visible
    // (a real Spark/Glue stage is minutes; this keeps the local demo honest about timing).
    long stepDelayMillis = longEnv("TRANSFORM_STEP_DELAY_MILLIS", 400);
    long pollMillis = longEnv("TRANSFORM_POLL_MILLIS", 300);
    TransformJobRunner transformJob = new TransformJobRunner(s3, stepDelayMillis);

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(cfg.taskQueue());
    worker.registerWorkflowImplementationTypes(ImportWorkflowImpl.class);
    worker.registerActivitiesImplementations(
        new ImportActivitiesImpl(s3, sns, ssm, transformJob, pollMillis),
        new SecretActivitiesImpl(ssm));

    // Lab 6.6: start the SQS trigger bridge on its own daemon thread (unless disabled).
    SqsSignalBridge bridge = null;
    boolean bridgeEnabled = !"false".equalsIgnoreCase(System.getenv("BRIDGE_ENABLED"));
    if (bridgeEnabled && !Config.IMPORTS_EVENTS_QUEUE_URL.isBlank()) {
      bridge = new SqsSignalBridge(client, sqs, Config.IMPORTS_EVENTS_QUEUE_URL, cfg.taskQueue());
      Thread t = new Thread(bridge, "sqs-trigger-bridge");
      t.setDaemon(true);
      t.start();
    }

    // Lab 6.4: drain in-flight work on SIGTERM (docker stop / pod delete) rather than aborting.
    final SqsSignalBridge bridgeRef = bridge;
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  System.out.println("Shutting down worker...");
                  if (bridgeRef != null) {
                    bridgeRef.stop();
                  }
                  factory.shutdown();
                }));

    factory.start();
    System.out.printf(
        "Worker started. target=%s namespace=%s taskQueue=%s bridge=%s%n",
        cfg.temporalAddress(), cfg.namespace(), cfg.taskQueue(), bridge != null);
  }

  private static long longEnv(String key, long fallback) {
    String v = System.getenv(key);
    if (v == null || v.isBlank()) {
      return fallback;
    }
    try {
      return Long.parseLong(v);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }
}
