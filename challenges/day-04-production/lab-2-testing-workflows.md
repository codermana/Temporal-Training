# Lab 4.2 — Testing Workflows

**Time:** ~45 min · **Difficulty:** ★★ · **Stack:** None (in-process)

## Scenario

A reminder Workflow sleeps for a day, then returns a message. You can't wait a
day in a unit test — and you shouldn't need a running server either. Temporal's
`TestWorkflowEnvironment` runs Workflows in-process and **skips time**, so a
one-day sleep completes in milliseconds. You'll write a JUnit 5 test that proves
the Workflow's behavior, then mock an Activity.

## Learning goals

- Run a Workflow in-process with `TestWorkflowEnvironment` (no Docker, no server).
- Skip Workflow time deterministically (`testEnv.sleep(...)`).
- Mock Activities with Mockito so tests don't depend on real side effects.
- Structure tests with `@RegisterExtension TestWorkflowExtension`.

> **Coming from Airflow `[airflow]`:** there's no DAG-bag parse test or
> `airflow tasks test` against a live metastore. This is a real unit test of
> orchestration logic — fast, hermetic, deterministic.

## Prerequisites

- JUnit 5 and the Temporal testing dependency. No server needed.

## Starter code

Module `training.temporal.testing`. Add the test dep to `pom.xml`:

```xml
<dependency>
  <groupId>io.temporal</groupId>
  <artifactId>temporal-testing</artifactId>
  <version>1.32.1</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.junit.jupiter</groupId>
  <artifactId>junit-jupiter</artifactId>
  <version>5.10.2</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.mockito</groupId>
  <artifactId>mockito-core</artifactId>
  <version>5.11.0</version>
  <scope>test</scope>
</dependency>
```

**Given — the Workflow under test** (`src/main/java/...`):

```java
// ReminderWorkflow.java
@WorkflowInterface
public interface ReminderWorkflow {
  @WorkflowMethod
  String remindAfterOneDay(String message);
}
```

```java
// ReminderWorkflowImpl.java
public class ReminderWorkflowImpl implements ReminderWorkflow {
  @Override
  public String remindAfterOneDay(String message) {
    Workflow.sleep(Duration.ofDays(1));
    return "Reminder: " + message;
  }
}
```

**Your job — the test** (`src/test/java/...`):

```java
// ReminderWorkflowTest.java
class ReminderWorkflowTest {

  @Test
  void skipsWorkflowTime() {
    String taskQueue = "test-reminder";
    try (TestWorkflowEnvironment testEnv = TestWorkflowEnvironment.newInstance()) {
      // TODO 1: create a Worker on taskQueue and register ReminderWorkflowImpl.
      // TODO 2: testEnv.start().
      // TODO 3: build a typed stub from testEnv.getWorkflowClient().
      // TODO 4: start the workflow (WorkflowClient.start(...)).
      // TODO 5: testEnv.sleep(Duration.ofDays(1)) to skip the wait.
      // TODO 6: fetch the result and assertEquals("Reminder: ship report", ...).
    }
  }
}
```

## Tasks

1. Complete `skipsWorkflowTime` and confirm it passes in **well under a second**
   despite the one-day sleep.
2. Add a second Workflow that calls an Activity, and write a test that **mocks**
   that Activity with Mockito (`mock(...)` + `when(...).thenReturn(...)`),
   registering the mock via `worker.registerActivitiesImplementations(mock)`.
3. Add a negative test: make the mocked Activity throw and assert the Workflow
   surfaces the failure as expected.

## Verification

```bash
mvn -q test
```

<details><summary>Under the hood — what <code>make run-testing</code> runs</summary>

```bash
cd examples/runnable/06-testing && mvn -q test
# Pure in-process test — no Temporal server needed.
```

</details>

Expected: green tests, total runtime dominated by JVM/Maven startup — the
one-day sleep adds no wall-clock time.

## Definition of done

- [ ] The time-skipping test passes quickly (no real 24h wait).
- [ ] An Activity-mocking test passes using a Mockito mock registered on the
      test Worker.
- [ ] A failure-path test asserts the Workflow's behavior when an Activity throws.

## Pitfalls

- **`Thread.sleep` ≠ `Workflow.sleep`.** Only `Workflow.sleep` participates in
  time-skipping; a real `Thread.sleep` in Workflow code is both non-deterministic
  and not skippable.
- Build the stub from **`testEnv.getWorkflowClient()`**, not a real
  `WorkflowClient` against `127.0.0.1:7233`.
- Close the environment (try-with-resources) so the in-process server shuts down
  between tests.

## Hints

<details><summary>Hint 1 — fetching the result after time-skip</summary>

Start async, skip time, then read:
```java
var exec = WorkflowClient.start(workflow::remindAfterOneDay, "ship report");
testEnv.sleep(Duration.ofDays(1));
String result = client.newUntypedWorkflowStub(exec.getWorkflowId()).getResult(String.class);
```
</details>

<details><summary>Hint 2 — TestWorkflowExtension alternative</summary>

For less boilerplate, use
`@RegisterExtension static final TestWorkflowExtension EXT = TestWorkflowExtension
.newBuilder().setWorkflowTypes(ReminderWorkflowImpl.class).build();` and inject
the stub/`TestWorkflowEnvironment` as test-method parameters.
</details>

## Stretch goals

- Test a Signal: start a waiting Workflow, send a signal in the test, and assert
  it completes.
- Use `TestWorkflowExtension` to write a Spring Boot-style integration test
  (foreshadows Day 5).
