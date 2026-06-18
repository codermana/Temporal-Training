package training.temporal.aws;

/**
 * The Worker's Temporal connection config, loaded once from SSM Parameter Store at
 * startup (Lab 6.8) — an immutable snapshot, not re-read per task. These values
 * are deliberately <i>not</i> in {@link Config}: they come from the parameter tree
 * so config changes don't need an image rebuild.
 */
public record WorkerConfig(String temporalAddress, String namespace, String taskQueue) {}
