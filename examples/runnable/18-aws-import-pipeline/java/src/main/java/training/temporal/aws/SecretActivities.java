package training.temporal.aws;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * The per-run secret, fetched at use-time inside an Activity (Lab 6.8). Exposed as
 * its own interface so a Workflow step that genuinely needs the key can call it
 * (and so it shows as its own {@code ActivityTaskCompleted} in the Web UI). The
 * default {@link ImportWorkflow} reads the key <i>inside</i> {@code load} instead,
 * so the plaintext never enters Workflow history — but both are registered.
 */
@ActivityInterface
public interface SecretActivities {

  /** Return the KMS-decrypted api-key. NEVER call this from Workflow code; NEVER log the value. */
  @ActivityMethod
  String fetchApiKey();
}
