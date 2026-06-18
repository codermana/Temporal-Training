package training.temporal.tracing;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.client.WorkflowOptions;
import io.temporal.opentracing.OpenTracingClientInterceptor;
import io.temporal.serviceclient.WorkflowServiceStubs;

/**
 * Standalone client: starts one OrderWorkflow and prints its result. The
 * client-side interceptor opens the root span ("StartWorkflow:OrderWorkflow")
 * and propagates its context to the server, so the Worker's spans nest under
 * it. Run {@link TracingWorker} first in another terminal.
 */
public class TracingStarter {
  private static final String TASK_QUEUE = "distributed-tracing";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();

    WorkflowClient client =
        WorkflowClient.newInstance(
            service,
            WorkflowClientOptions.newBuilder()
                .setInterceptors(
                    new OpenTracingClientInterceptor(
                        Telemetry.openTracingOptions("temporal-client-java")))
                .build());

    String orderId = args.length > 0 ? args[0] : "A-1001";

    OrderWorkflow workflow =
        client.newWorkflowStub(
            OrderWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("order-" + orderId)
                .setTaskQueue(TASK_QUEUE)
                .build());

    String result = workflow.process(orderId);
    System.out.println(result);
    System.out.println("Open the trace in Jaeger: http://localhost:16686 (service temporal-client-java)");
    System.exit(0);
  }
}
