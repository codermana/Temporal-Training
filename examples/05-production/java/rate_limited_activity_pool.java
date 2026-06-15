// NEW production snippet: rate-limit a pool of Workers calling a fragile
// downstream (a legacy API, a vendor with a QPS cap). Two complementary knobs:
//   setMaxActivitiesPerSecond              -> per-Worker cap
//   setMaxTaskQueueActivitiesPerSecond     -> GLOBAL cap across every Worker
//                                             polling this task queue (server-enforced)
// Use the task-queue-wide limit to protect the dependency no matter how many
// Worker replicas you scale to.
class RateLimitedWorkerBootstrap {
  void start(WorkflowClient client) {
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker =
        factory.newWorker(
            "legacy-api-calls",
            WorkerOptions.newBuilder()
                // Cap THIS worker to 50 activity starts/sec...
                .setMaxActivitiesPerSecond(50)
                // ...and the whole task queue (all replicas combined) to 100/sec,
                // so the vendor never sees more than 100 QPS even at 20 replicas.
                .setMaxTaskQueueActivitiesPerSecond(100)
                .build());

    worker.registerActivitiesImplementations(new LegacyApiActivitiesImpl());
    factory.start();
  }
}
