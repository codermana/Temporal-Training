package training.temporal.aws;

import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

/** Reads the SecureString api-key from SSM (KMS-decrypted) and helps keep it out of logs (Lab 6.8). */
public final class Secrets {

  private Secrets() {}

  /** Fetch + decrypt the api-key. Read this at use-time inside an Activity, never in Workflow code. */
  public static String apiKey(SsmClient ssm) {
    return ssm.getParameter(
            GetParameterRequest.builder().name(Config.SSM_API_KEY).withDecryption(true).build())
        .parameter()
        .value();
  }

  /** Mask a secret for logging: keep a 4-char prefix, redact the rest. Never log the raw value. */
  public static String mask(String secret) {
    if (secret == null || secret.isEmpty()) {
      return "(none)";
    }
    int keep = Math.min(4, secret.length());
    return secret.substring(0, keep) + "****";
  }
}
