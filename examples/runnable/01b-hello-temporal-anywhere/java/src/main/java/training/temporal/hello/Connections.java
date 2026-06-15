package training.temporal.hello;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.SimpleSslContextBuilder;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Builds a {@link WorkflowClient} from environment variables so the same Worker can target a local
 * dev server, a Dockerized cluster, or Temporal Cloud without code changes. This is the shared base
 * both Lab 1.2b (Docker) and Lab 1.2c (Cloud) build on — only the environment differs.
 *
 * <p>Connection mode is selected by which variables are set:
 *
 * <ul>
 *   <li><b>API key</b> (Cloud) — {@code TEMPORAL_API_KEY} set. Connects over TLS to the regional
 *       gRPC endpoint in {@code TEMPORAL_ADDRESS} (e.g. {@code us-east-1.aws.api.temporal.io:7233}).
 *   <li><b>mTLS</b> (Cloud) — {@code TEMPORAL_TLS_CERT} + {@code TEMPORAL_TLS_KEY} set. Connects to
 *       the namespace endpoint in {@code TEMPORAL_ADDRESS} (e.g. {@code ns.acct.tmprl.cloud:7233}).
 *   <li><b>Plaintext</b> (local dev server or Docker cluster) — no credentials set. Connects to
 *       {@code TEMPORAL_ADDRESS}, defaulting to {@code 127.0.0.1:7233} / namespace {@code default},
 *       so it runs unchanged against {@code make temporal} or {@code make stack-temporal}.
 * </ul>
 */
public final class Connections {
  private Connections() {}

  public static WorkflowClient fromEnv() {
    String address = getenv("TEMPORAL_ADDRESS", "127.0.0.1:7233");
    String namespace = getenv("TEMPORAL_NAMESPACE", "default");
    String apiKey = System.getenv("TEMPORAL_API_KEY");
    String tlsCert = System.getenv("TEMPORAL_TLS_CERT");
    String tlsKey = System.getenv("TEMPORAL_TLS_KEY");

    WorkflowServiceStubsOptions.Builder stubs = WorkflowServiceStubsOptions.newBuilder();

    if (isSet(apiKey)) {
      // --- Temporal Cloud via API key (simplest) -------------------------------------------
      System.out.printf("Connecting with API key to %s (namespace %s)%n", address, namespace);
      stubs
          .setTarget(address)
          .setEnableHttps(true)
          .addApiKey(() -> apiKey);
    } else if (isSet(tlsCert) && isSet(tlsKey)) {
      // --- Temporal Cloud via mTLS client certificate --------------------------------------
      System.out.printf("Connecting with mTLS to %s (namespace %s)%n", address, namespace);
      try (InputStream cert = new FileInputStream(tlsCert);
          InputStream key = new FileInputStream(tlsKey)) {
        stubs.setSslContext(SimpleSslContextBuilder.forPKCS8(cert, key).build());
      } catch (Exception e) {
        throw new RuntimeException("Failed to load mTLS cert/key", e);
      }
      stubs.setTarget(address);
    } else {
      // --- Local dev server (no credentials) -----------------------------------------------
      System.out.printf("Connecting to local server at %s (namespace %s)%n", address, namespace);
      stubs.setTarget(address);
    }

    WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(stubs.build());
    return WorkflowClient.newInstance(
        service, WorkflowClientOptions.newBuilder().setNamespace(namespace).build());
  }

  private static String getenv(String key, String fallback) {
    String v = System.getenv(key);
    return isSet(v) ? v : fallback;
  }

  private static boolean isSet(String v) {
    return v != null && !v.isBlank();
  }
}
