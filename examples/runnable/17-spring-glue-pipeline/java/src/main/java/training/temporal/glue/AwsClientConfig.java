package training.temporal.glue;

import java.net.URI;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * The AWS SDK clients as Spring beans. Unlike the Temporal client, these are not
 * auto-configured by any starter, so they live in a small {@code @Configuration}.
 *
 * <p>When {@code aws.endpoint} is set (the local default,
 * {@code http://127.0.0.1:4566}) the clients point at LocalStack with static
 * {@code test/test} credentials. Clear {@code aws.endpoint} (empty string) and
 * they fall back to the SDK's default endpoint resolver + the
 * {@link DefaultCredentialsProvider} chain — i.e. the ECS task role or EKS IRSA
 * on real AWS, so there are no static keys in production.
 *
 * <p>S3 uses <b>path-style</b> access because LocalStack serves buckets as
 * {@code http://host:4566/<bucket>} rather than {@code <bucket>.s3...}.
 */
@Configuration
public class AwsClientConfig {

  @Value("${aws.endpoint:}")
  private String endpoint;

  @Value("${aws.region:us-east-1}")
  private String region;

  private boolean localStack() {
    return StringUtils.hasText(endpoint);
  }

  /** test/test for LocalStack; the default provider chain (task role / IRSA) on real AWS. */
  private <B extends software.amazon.awssdk.awscore.client.builder.AwsClientBuilder<B, ?>> B base(B builder) {
    builder.region(Region.of(region));
    if (localStack()) {
      builder
          .endpointOverride(URI.create(endpoint))
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }
    return builder;
  }

  @Bean(destroyMethod = "close")
  public S3Client s3Client() {
    return base(S3Client.builder())
        .forcePathStyle(true) // LocalStack serves http://host:4566/<bucket>/<key>
        .build();
  }

  @Bean(destroyMethod = "close")
  public SqsClient sqsClient() {
    return base(SqsClient.builder()).build();
  }

  @Bean(destroyMethod = "close")
  public SnsClient snsClient() {
    return base(SnsClient.builder()).build();
  }
}
