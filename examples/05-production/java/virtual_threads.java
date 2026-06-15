class VirtualThreadWorker {
  void platformThreadWorker(WorkflowClient client) {
    WorkerFactory factory = WorkerFactory.newInstance(client);

    Worker worker =
        factory.newWorker(
            "blocking-activities",
            WorkerOptions.newBuilder()
                .setMaxConcurrentActivityExecutionSize(100)
                .build());

    worker.registerActivitiesImplementations(new BlockingIoActivitiesImpl());
    factory.start();
  }

  void virtualThreadWorker(WorkflowClient client) {
    WorkerFactoryOptions factoryOptions =
        WorkerFactoryOptions.newBuilder().setUsingVirtualWorkflowThreads(true).build();

    WorkerFactory factory = WorkerFactory.newInstance(client, factoryOptions);
    Worker worker =
        factory.newWorker(
            "high-concurrency-activities",
            WorkerOptions.newBuilder()
                .setUsingVirtualThreads(true)
                .setMaxConcurrentActivityExecutionSize(1_000)
                .build());

    worker.registerActivitiesImplementations(new BlockingIoActivitiesImpl());
    factory.start();
  }
}
