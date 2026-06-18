package training.temporal.aws;

import software.amazon.awssdk.services.ssm.SsmClient;

/** Reads the SecureString api-key from SSM Parameter Store, KMS-decrypted, at use-time (Lab 6.8). */
public class SecretActivitiesImpl implements SecretActivities {

  private final SsmClient ssm;

  public SecretActivitiesImpl(SsmClient ssm) {
    this.ssm = ssm;
  }

  @Override
  public String fetchApiKey() {
    return Secrets.apiKey(ssm);
  }
}
