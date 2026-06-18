package training.temporal.glue;

/** Single source of truth for the task queue shared by Workflow, Activities, and the bridge. */
final class GlueStitchConstants {
  static final String TASK_QUEUE = "glue-stitch";

  private GlueStitchConstants() {}
}
