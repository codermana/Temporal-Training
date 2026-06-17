package training.temporal.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A minimal Temporal + Spring Boot application.
 *
 * <p>There is no manual {@code @Configuration} here: the
 * {@code temporal-spring-boot-starter} reads {@code application.yml}, builds the
 * {@link io.temporal.client.WorkflowClient}, discovers every {@code @WorkflowImpl}
 * / {@code @ActivityImpl} in the configured package, and starts a Worker for the
 * task queue they declare — all bound to the Spring lifecycle (graceful shutdown
 * included). We just inject the {@code WorkflowClient} where we need it.
 */
@SpringBootApplication
public class SpringBootApp {
  public static void main(String[] args) {
    SpringApplication.run(SpringBootApp.class, args);
  }
}
