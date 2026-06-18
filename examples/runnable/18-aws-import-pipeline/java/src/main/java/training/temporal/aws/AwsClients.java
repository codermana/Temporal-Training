package training.temporal.aws;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.ssm.SsmClient;

/**
 * Factory for the AWS SDK v2 clients this pipeline uses (S3, SQS, SNS, SSM),
 * configured once for either LocalStack or real AWS.
 *
 * <p>When {@code AWS_ENDPOINT} is set (the local default,
 * {@code http://127.0.0.1:4566}) the clients point at LocalStack with static
 * {@code test/test} credentials. Clear {@code AWS_ENDPOINT} (empty string) and
 * they fall back to the SDK's default endpoint resolver + the
 * {@link DefaultCredentialsProvider} chain — i.e. the ECS task role or EKS IRSA
 * on real AWS, so there are no static keys baked into the image (Lab 6.8).
 *
 * <p>This is the single LocalStack-vs-AWS seam the Day-6 labs keep pointing at:
 * the same builder shape backs the {@code GlueClient}/{@code S3Client}/
 * {@code SqsClient}/{@code SnsClient}/{@code SsmClient} hints in labs 6.1–6.8.
 */
public final class AwsClients {

  private AwsClients() {}

  private static boolean localStack() {
    return !Config.AWS_ENDPOINT.isBlank();
  }

  /** test/test for LocalStack; the default provider chain (task role / IRSA) on real AWS. */
  private static <B extends AwsClientBuilder<B, ?>> B base(B builder) {
    builder.region(Region.of(Config.AWS_REGION));
    if (localStack()) {
      builder
          .endpointOverride(URI.create(Config.AWS_ENDPOINT))
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }
    return builder;
  }

  public static S3Client s3() {
    // Path-style: LocalStack serves http://host:4566/<bucket>/<key>, not vhost-style.
    return base(S3Client.builder()).forcePathStyle(true).build();
  }

  public static SqsClient sqs() {
    return base(SqsClient.builder()).build();
  }

  public static SnsClient sns() {
    return base(SnsClient.builder()).build();
  }

  public static SsmClient ssm() {
    return base(SsmClient.builder()).build();
  }
}
