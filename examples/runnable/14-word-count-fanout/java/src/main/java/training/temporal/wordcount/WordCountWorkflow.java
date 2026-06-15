package training.temporal.wordcount;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.List;

@WorkflowInterface
public interface WordCountWorkflow {
  @WorkflowMethod
  int count(List<String> chunks);
}
