class AsyncProcedureAndRaceWorkflow {
  private final NotificationActivities notify =
      Workflow.newActivityStub(
          NotificationActivities.class,
          ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofMinutes(1)).build());

  // Async.procedure: fire a void Activity without blocking the Workflow loop.
  // (Async.function is for Activities that return a value.)
  void notifyEveryone(List<String> userIds) {
    List<Promise<Void>> sends =
        userIds.stream().map(id -> Async.procedure(notify::send, id)).toList();
    Promise.allOf(sends).get();
  }

  // Promise.anyOf: continue as soon as the FIRST branch finishes
  // (e.g. primary vs fallback provider). allOf waits for every branch.
  String firstToAnswer(String query) {
    Promise<String> primary = Async.function(notify::askPrimary, query);
    Promise<String> fallback = Async.function(notify::askFallback, query);
    Promise.anyOf(primary, fallback).get();
    return primary.isCompleted() ? primary.get() : fallback.get();
  }
}
