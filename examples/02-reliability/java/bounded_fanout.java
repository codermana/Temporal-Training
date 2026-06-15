// Real-world: fan out 10k notifications, but a downstream provider only
// tolerates ~20 in-flight calls. Unbounded Async.function would schedule all
// 10k at once. A deterministic in-Workflow counter gated by Workflow.await caps
// concurrency while keeping the pipeline full — the durable equivalent of a
// bounded thread pool. (Don't use java.util.concurrent.Semaphore here: blocking
// a real thread isn't replay-safe. Workflow.await is the deterministic wait.)
class BoundedFanoutWorkflow {
  private final NotificationActivities notify =
      Workflow.newActivityStub(
          NotificationActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofMinutes(1)).build());

  int sendAll(List<String> userIds, int maxInFlight) {
    int[] inFlight = {0};
    List<Promise<Void>> sends = new ArrayList<>();

    for (String id : userIds) {
      // Park the Workflow (deterministically) until a slot frees up.
      Workflow.await(() -> inFlight[0] < maxInFlight);
      inFlight[0]++;
      Promise<Void> send =
          Async.procedure(notify::send, id)
              .thenApply(ignored -> { inFlight[0]--; return null; });
      sends.add(send);
    }

    Promise.allOf(sends).get();
    return userIds.size();
  }
}
