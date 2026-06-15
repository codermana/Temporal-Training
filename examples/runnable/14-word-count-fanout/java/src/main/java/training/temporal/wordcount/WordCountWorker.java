package training.temporal.wordcount;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Standalone Worker: registers the Workflow + Activities and polls the
 * word-count Task Queue. Start a run from another terminal with
 * {@link WordCountStarter}.
 */
public class WordCountWorker {
  static final String TASK_QUEUE = "word-count";

  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker = factory.newWorker(TASK_QUEUE);
    worker.registerWorkflowImplementationTypes(WordCountWorkflowImpl.class);
    worker.registerActivitiesImplementations(new WordCountActivitiesImpl());

    factory.start();
    System.out.println("Worker started on task queue '" + TASK_QUEUE + "'. Ctrl-C to stop.");
  }
}
