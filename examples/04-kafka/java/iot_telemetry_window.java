class TelemetryWindowWorkflow implements DeviceTelemetryWorkflow {
  private final TelemetrySink sink =
      Workflow.newActivityStub(
          TelemetrySink.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofSeconds(15)).build());

  // One long-lived Workflow per IoT device. Each Kafka telemetry record arrives
  // as a Signal; the Workflow buffers readings and, every fixed window, publishes
  // a rollup to a `device-rollups` topic. Workflow.sleep is the replay-safe timer
  // that closes the window — never a wall-clock sleep.
  private final List<Double> readings = new ArrayList<>();

  @Override
  public void run(String deviceId) {
    int windows = 0;
    while (true) {
      Workflow.sleep(Duration.ofMinutes(1));
      if (!readings.isEmpty()) {
        double avg = readings.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        sink.publishRollup("device-rollups", deviceId, avg, readings.size());
        readings.clear();
      }
      if (++windows >= 60) {
        Workflow.continueAsNew(deviceId);
      }
    }
  }

  @Override
  public void onReading(double value) {
    readings.add(value);
  }
}
