package training.temporal.aws;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;

public final class WorkerMain {

  private WorkerMain() {}

  public static void main(String[] args) {
    String target = System.getenv().getOrDefault("TEMPORAL_ADDRESS", "127.0.0.1:7233");
    String namespace = System.getenv().getOrDefault("TEMPORAL_NAMESPACE", "default");
    String taskQueue = System.getenv().getOrDefault("TASK_QUEUE", "transform");

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(target).build());

    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            io.temporal.client.WorkflowClientOptions.newBuilder().setNamespace(namespace).build());

    // Optional: cap how many Activities one Worker runs at once. Left at the SDK
    // default normally; the Day-6 KEDA lab sets it low (e.g. 2) so Task Queue
    // backlog builds faster than one pod can drain it, making autoscale visible.
    WorkerOptions.Builder workerOptions = WorkerOptions.newBuilder();
    String maxActivities = System.getenv("MAX_CONCURRENT_ACTIVITIES");
    if (maxActivities != null && !maxActivities.isBlank()) {
      workerOptions.setMaxConcurrentActivityExecutionSize(Integer.parseInt(maxActivities.trim()));
    }

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(taskQueue, workerOptions.build());
    worker.registerWorkflowImplementationTypes(ImportWorkflowImpl.class);
    worker.registerActivitiesImplementations(new ImportActivitiesImpl());

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Shutting down worker...");
      factory.shutdown();
    }));

    factory.start();
    System.out.printf(
        "Worker started. target=%s namespace=%s taskQueue=%s%n", target, namespace, taskQueue);
  }
}
