package training.temporal.continueasnew;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

public class ContinueAsNewWorker {
  private static final String TASK_QUEUE = "continue-as-new";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(CounterWorkflowImpl.class);
    factory.start();

    CounterWorkflow workflow =
        client.newWorkflowStub(
            CounterWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TASK_QUEUE)
                .setWorkflowId("continue-as-new-demo")
                .build());

    // getResult transparently follows the chain of continue-as-new runs to the final result.
    WorkflowClient.start(workflow::count, 0);
    String result = WorkflowStub.fromTyped(workflow).getResult(String.class);
    System.out.println("Result: " + result);
    System.out.println(
        "In the Web UI, the single Workflow ID shows multiple Runs chained by ContinueAsNew.");

    factory.shutdown();
  }
}
