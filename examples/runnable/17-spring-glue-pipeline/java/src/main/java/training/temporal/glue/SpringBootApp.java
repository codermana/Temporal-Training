package training.temporal.glue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot service that orchestrates an AWS Glue job with Temporal.
 *
 * <p>The story (see README and the Day-6 slides): a producer "A" writes Parquet
 * partitions to S3 and drops a message on an SQS "bus". This service bridges that
 * SQS event into a {@code signalWithStart} on {@link GlueStitchWorkflow}, which
 * validates the partition in S3, triggers and supervises a Glue job that stitches
 * the data, then publishes an SNS notification back to A.
 *
 * <p>There is no manual Temporal {@code @Configuration}: the
 * {@code temporal-spring-boot-starter} reads {@code application.yml}, builds the
 * {@link io.temporal.client.WorkflowClient}, discovers the {@code @WorkflowImpl} /
 * {@code @ActivityImpl} beans, and starts a Worker — all bound to the Spring
 * lifecycle. The only hand-wired config is the AWS SDK clients
 * ({@link AwsClientConfig}), which Temporal knows nothing about.
 */
@SpringBootApplication
public class SpringBootApp {
  public static void main(String[] args) {
    SpringApplication.run(SpringBootApp.class, args);
  }
}
