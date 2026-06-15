package training.temporal.wordcount;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import java.util.List;

/**
 * Standalone client: starts one WordCountWorkflow and prints its result. Run
 * {@link WordCountWorker} first in another terminal so there is something
 * polling the Task Queue.
 */
public class WordCountStarter {
  public static void main(String[] args) {
    WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
    WorkflowClient client = WorkflowClient.newInstance(service);

    WordCountWorkflow workflow =
        client.newWorkflowStub(
            WordCountWorkflow.class,
            WorkflowOptions.newBuilder()
                .setWorkflowId("word-count-demo")
                .setTaskQueue(WordCountWorker.TASK_QUEUE)
                .build());

    int total = workflow.count(SampleText.chunks());
    System.out.println("Total words: " + total);
    System.exit(0);
  }
}
