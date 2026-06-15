package training.temporal.wordcount;

import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.List;

public class WordCountWorkflowImpl implements WordCountWorkflow {
  private final WordCountActivities activities =
      Workflow.newActivityStub(
          WordCountActivities.class,
          ActivityOptions.newBuilder()
              .setStartToCloseTimeout(Duration.ofSeconds(30))
              .setRetryOptions(
                  RetryOptions.newBuilder()
                      .setInitialInterval(Duration.ofSeconds(1))
                      .setMaximumAttempts(3)
                      .build())
              .build());

  @Override
  public int count(List<String> chunks) {
    List<Promise<Integer>> counts =
        chunks.stream().map(chunk -> Async.function(activities::countWords, chunk)).toList();

    Promise.allOf(counts).get();
    return counts.stream().mapToInt(Promise::get).sum();
  }
}
