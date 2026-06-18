package training.temporal.aws;

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathRequest;
import software.amazon.awssdk.services.ssm.model.GetParametersByPathResponse;
import software.amazon.awssdk.services.ssm.model.Parameter;

/**
 * Loads the Worker's Temporal connection config from SSM Parameter Store at
 * startup (Lab 6.8). Reading SSM here is fine: this is <b>process startup code</b>,
 * not Workflow code, so the non-deterministic read can't break replay. The tree
 * is read once into an immutable {@link WorkerConfig} snapshot, not per task.
 *
 * <p>If SSM is unreachable (no LocalStack / no params seeded), it falls back to
 * env-var defaults so the module still boots — handy for a bare {@code java -jar}.
 */
public final class WorkerBootstrap {

  private WorkerBootstrap() {}

  public static WorkerConfig loadConfig(SsmClient ssm) {
    Map<String, String> params = new HashMap<>();
    String nextToken = null;
    do {
      GetParametersByPathResponse resp =
          ssm.getParametersByPath(
              GetParametersByPathRequest.builder()
                  .path(Config.SSM_PATH)
                  .recursive(true)
                  .withDecryption(true) // SecureStrings come back decrypted (KMS)
                  .nextToken(nextToken) // null on the first call
                  .build());
      for (Parameter p : resp.parameters()) {
        // Key each param by its trailing name: ".../task-queue" -> "task-queue".
        String name = p.name().substring(p.name().lastIndexOf('/') + 1);
        params.put(name, p.value());
      }
      nextToken = resp.nextToken();
    } while (nextToken != null);

    return new WorkerConfig(
        params.getOrDefault("temporal-address", env("TEMPORAL_ADDRESS", "127.0.0.1:7233")),
        params.getOrDefault("namespace", env("TEMPORAL_NAMESPACE", "default")),
        params.getOrDefault("task-queue", env("TASK_QUEUE", "transform")));
  }

  /** Best-effort config: try SSM, fall back to env defaults if the read fails. */
  public static WorkerConfig loadConfigOrDefaults(SsmClient ssm) {
    try {
      return loadConfig(ssm);
    } catch (Exception e) {
      return new WorkerConfig(
          env("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
          env("TEMPORAL_NAMESPACE", "default"),
          env("TASK_QUEUE", "transform"));
    }
  }

  private static String env(String key, String fallback) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? fallback : v;
  }
}
