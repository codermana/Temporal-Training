// Real-world: a multi-hour data backfill that must resume where it left off
// after a worker crash or activity retry — not restart from page 0. The
// heartbeat carries the last completed page as its "details"; on a retry the
// next attempt reads that detail back and skips the work already done.
class BackfillActivitiesImpl implements BackfillActivities {
  @Override
  public String backfill(String dataset) {
    ActivityExecutionContext ctx = Activity.getExecutionContext();

    // Resume point: the details attached to the most recent heartbeat of the
    // PREVIOUS attempt, if this is a retry. Empty on the first attempt.
    int startPage =
        ctx.getHeartbeatDetails(Integer.class).orElse(0);

    for (int page = startPage; page < 100_000; page++) {
      copyPage(dataset, page);
      ctx.heartbeat(page); // checkpoint: this page is done
    }
    return "backfilled " + dataset;
  }
}
