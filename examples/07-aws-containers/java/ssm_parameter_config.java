// A containerized Worker needs config (Temporal address/namespace, task queue,
// downstream endpoints) and secrets (an API key) without baking them into the
// image or shipping them as plaintext env vars. In AWS this is "env vars /
// mounted secrets file"; here it's SSM Parameter Store read at WORKER STARTUP.
// getParametersByPath loads a whole "/temporal-training/worker/" tree at boot,
// WithDecryption decrypts SecureString params via KMS, and the values build the
// WorkflowServiceStubs + WorkerFactory.
//
// The boundary that matters for Temporal: this read happens in plain process
// (bootstrap) code, NOT in a Workflow — an SSM call is non-deterministic I/O and
// the value can change between replays. When a Workflow STEP needs a secret, it
// reads it inside the fetchApiKey Activity, never in Workflow code. In ECS/EKS
// the SsmClient authenticates via the task role / IRSA, so there are no static
// keys; the LocalStack endpoint override below stands in for that locally.
class SsmParameterConfig {

  // Immutable snapshot of the config tree, cached at boot — we read SSM once at
  // startup, not per task, so we don't hammer Parameter Store under load.
  record WorkerConfig(String temporalAddress, String namespace, String taskQueue, String orderApiUrl) {}

  // Point at LocalStack; in ECS/EKS drop the endpoint override and let the task
  // role / IRSA supply credentials (same builder shape as Lab 1's GlueClient).
  static SsmClient ssmForLocalStack() {
    return SsmClient.builder()
        .endpointOverride(URI.create("http://127.0.0.1:4566"))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
        .build();
  }

  // Load the whole config tree at startup. WithDecryption=true decrypts any
  // SecureString params via KMS; getParametersByPath paginates, so follow
  // NextToken until it's null to be sure every key is loaded.
  static WorkerConfig loadConfig(SsmClient ssm, String path) {
    Map<String, String> params = new HashMap<>();
    String nextToken = null;
    do {
      GetParametersByPathResponse resp =
          ssm.getParametersByPath(
              GetParametersByPathRequest.builder()
                  .path(path) // "/temporal-training/worker/"
                  .recursive(true)
                  .withDecryption(true)
                  .nextToken(nextToken)
                  .build());
      for (Parameter p : resp.parameters()) {
        // key by the trailing name: "/temporal-training/worker/namespace" -> "namespace"
        params.put(p.name().substring(p.name().lastIndexOf('/') + 1), p.value());
      }
      nextToken = resp.nextToken();
    } while (nextToken != null);

    return new WorkerConfig(
        params.get("temporal-address"),
        params.getOrDefault("namespace", "default"),
        params.get("task-queue"),
        params.get("order-api-url"));
  }

  // Bootstrap: read config from SSM, then build the Temporal stubs and Worker
  // from those values. This is process startup code — reading SSM here is fine.
  static WorkerFactory bootstrap() {
    WorkerConfig cfg;
    try (SsmClient ssm = ssmForLocalStack()) {
      cfg = loadConfig(ssm, "/temporal-training/worker/");
    }

    WorkflowServiceStubs service =
        WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder().setTarget(cfg.temporalAddress()).build());
    WorkflowClient client =
        WorkflowClient.newInstance(
            service, WorkflowClientOptions.newBuilder().setNamespace(cfg.namespace()).build());

    WorkerFactory factory = WorkerFactory.newInstance(client);
    Worker worker = factory.newWorker(cfg.taskQueue());
    worker.registerActivitiesImplementations(new SecretActivitiesImpl(ssmForLocalStack()));
    // worker.registerWorkflowImplementationTypes(...);
    return factory;
  }
}

@ActivityInterface
interface SecretActivities {
  @ActivityMethod
  String fetchApiKey(); // reads the SecureString param at use-time
}

// A Workflow step that needs the API key calls THIS, not SSM directly — the read
// is non-deterministic I/O and belongs behind the Activity boundary. The plaintext
// key stays inside the Activity; never log it and never return it into Workflow
// history if it can be avoided (pass it straight to the downstream call instead).
class SecretActivitiesImpl implements SecretActivities {
  private final SsmClient ssm;

  SecretActivitiesImpl(SsmClient ssm) {
    this.ssm = ssm;
  }

  @Override
  public String fetchApiKey() {
    return ssm.getParameter(
            GetParameterRequest.builder()
                .name("/temporal-training/worker/api-key")
                .withDecryption(true) // SecureString -> decrypted via KMS
                .build())
        .parameter()
        .value();
  }
}
