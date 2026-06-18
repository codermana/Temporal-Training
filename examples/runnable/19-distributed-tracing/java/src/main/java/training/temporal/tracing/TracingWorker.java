package training.temporal.tracing;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.opentracing.OpenTracingClientInterceptor;
import io.temporal.opentracing.OpenTracingOptions;
import io.temporal.opentracing.OpenTracingWorkerInterceptor;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;

/**
 * Standalone Worker with OpenTelemetry tracing. The OpenTracing/OpenTelemetry
 * Worker interceptor continues the trace started on the client into the
 * Workflow and each Activity, so the spans land in one Jaeger trace.
 *
 * <p>Needs a Temporal dev server on 127.0.0.1:7233 and Jaeger's OTLP receiver on
 * :4317 (run `make stack-trace`). Start a run from another terminal with {@link
 * TracingStarter}.
 */
public class TracingWorker {
  private static final String TASK_QUEUE = "distributed-tracing";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();

    // One exporter pipeline for the process; the client and worker interceptors
    // share it. The client interceptor goes on the WorkflowClient the factory is
    // built from; the worker interceptor goes on the factory.
    OpenTracingOptions otOptions = Telemetry.openTracingOptions("temporal-worker-java");

    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            WorkflowClientOptions.newBuilder()
                .setInterceptors(new OpenTracingClientInterceptor(otOptions))
                .build());

    WorkerFactory factory =
        WorkerFactory.newInstance(
            client,
            WorkerFactoryOptions.newBuilder()
                .setWorkerInterceptors(new OpenTracingWorkerInterceptor(otOptions))
                .build());

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(OrderWorkflowImpl.class);
    worker.registerActivitiesImplementations(new OrderActivitiesImpl());

    factory.start();
    System.out.println(
        "Tracing Worker started on task queue '"
            + TASK_QUEUE
            + "'. Traces -> Jaeger UI at http://localhost:16686. Ctrl-C to stop.");
  }
}
