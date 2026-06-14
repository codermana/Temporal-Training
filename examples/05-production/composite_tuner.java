class CompositeWorkerSizing {
  // CompositeTuner mixes slot strategies on one Worker: fixed slots for workflow
  // tasks (cheap, bursty) and resource-based slots for activities (the heavy work).
  void start(WorkflowClient client) {
    ResourceBasedController controller =
        ResourceBasedController.newSystemInfoController(
            ResourceBasedControllerOptions.newBuilder()
                .setTargetMemoryUsage(0.75)
                .setTargetCpuUsage(0.80)
                .build());

    WorkerTuner tuner =
        new CompositeTuner(
            new FixedSizeSlotSupplier<>(20), // workflow task slots
            ResourceBasedSlotSupplier.createForActivity( // activity slots
                controller, ResourceBasedSlotOptions.getDefaultInstance()),
            new FixedSizeSlotSupplier<>(20)); // local activity slots

    WorkerFactory.newInstance(client)
        .newWorker("mixed", WorkerOptions.newBuilder().setWorkerTuner(tuner).build());
  }
}
