class CronScheduleExample {
  // Airflow schedule_interval + catchup map directly onto a Temporal Schedule.
  void createDailyCron(ScheduleClient schedules) {
    Schedule schedule =
        Schedule.newBuilder()
            .setAction(
                ScheduleActionStartWorkflow.newBuilder()
                    .setWorkflowType(OrdersWorkflow.class)
                    .setOptions(WorkflowOptions.newBuilder().setTaskQueue("orders").build())
                    .setArguments("daily")
                    .build())
            .setSpec(
                ScheduleSpec.newBuilder()
                    // schedule_interval -> cron strings, intervals, OR calendars.
                    .setCronExpressions(List.of("0 9 * * *")) // 09:00 every day
                    .setJitter(Duration.ofMinutes(5))
                    .build())
            .setPolicy(
                SchedulePolicy.newBuilder()
                    // catchup -> bounded window in which missed runs are fired.
                    .setCatchupWindow(Duration.ofHours(1))
                    .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)
                    .build())
            .build();

    schedules.createSchedule("daily-orders-cron", schedule, ScheduleOptions.newBuilder().build());
  }
}

// Overlap policy = what happens when a run is still going when the next fires:
//   SKIP            - drop the new run (Airflow max_active_runs=1, catchup off)
//   BUFFER_ONE      - queue exactly one to run next
//   BUFFER_ALL      - queue every missed run
//   ALLOW_ALL       - run them concurrently
//   CANCEL_OTHER    - cancel the running one, then start
//   TERMINATE_OTHER - terminate the running one, then start
