package training.temporal.springboot;

import io.temporal.spring.boot.ActivityImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The Activity implementation is an ordinary Spring bean ({@code @Component}), so
 * it gets constructor injection, {@code @Value} properties, repositories, HTTP
 * clients — everything Spring offers. {@link ActivityImpl} hands this bean to the
 * starter, which registers it on the named task queue's Worker.
 */
@Component
@ActivityImpl(taskQueues = SpringBootConstants.TASK_QUEUE)
public class GreetingActivitiesImpl implements GreetingActivities {

  /** Proof that DI works inside an Activity: this comes from application.yml. */
  private final String greetingPrefix;

  public GreetingActivitiesImpl(@Value("${demo.greeting-prefix:Hello}") String greetingPrefix) {
    this.greetingPrefix = greetingPrefix;
  }

  @Override
  public String composeGreeting(String name) {
    return greetingPrefix + ", " + name + "! (from a Temporal Activity that is a Spring bean)";
  }
}
