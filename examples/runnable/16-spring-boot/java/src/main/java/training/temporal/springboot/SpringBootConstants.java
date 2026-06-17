package training.temporal.springboot;

/** Single source of truth for the task queue name shared by Workflow, Activity, and client. */
final class SpringBootConstants {
  static final String TASK_QUEUE = "greetings";

  private SpringBootConstants() {}
}
