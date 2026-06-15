class ReminderWorkflowTest {

  // TestWorkflowExtension wires the in-process test environment into JUnit 5 and
  // injects the env/worker/stub as test parameters. setDoNotStart lets us register
  // mocked Activities before starting.
  @RegisterExtension
  static final TestWorkflowExtension ext =
      TestWorkflowExtension.newBuilder()
          .setWorkflowTypes(ReminderWorkflowImpl.class)
          .setDoNotStart(true)
          .build();

  @Test
  void completesWithMockedActivity(
      TestWorkflowEnvironment env, Worker worker, ReminderWorkflow workflow) {
    // Mockito stands in for the real Activity implementation - no I/O in tests.
    ReminderActivities activities = mock(ReminderActivities.class);
    when(activities.lookupEmail("u1")).thenReturn("u1@example.com");
    worker.registerActivitiesImplementations(activities);
    env.start();

    String result = workflow.remind("u1"); // time-skipping still applies

    assertEquals("sent to u1@example.com", result);
    verify(activities).lookupEmail("u1");
  }
}
