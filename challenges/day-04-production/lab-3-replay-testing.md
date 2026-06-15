# Lab 4.3 — Replay testing

**Time:** ~40 min · **Difficulty:** ★★★ · **Stack:** None (replay only)

## Scenario

You're about to deploy a code change to a Workflow that has **executions already
running in production**. If your change alters the sequence of commands the
Workflow produces, replay against existing histories will fail — a non-deterministic
deployment that can wedge live Workflows. Replay testing catches this *before*
deploy: feed a real recorded history through your new code and assert it still
replays clean.

## Learning goals

- Capture a production-style history to JSON.
- Replay it against current code with `WorkflowReplayer`.
- Deliberately introduce a non-deterministic change and watch replay fail.
- Fix it the right way with `Workflow.getVersion()` (or
  `@WorkflowVersioningBehavior`).

## Prerequisites

- A Workflow with at least one **completed** execution whose history you can
  export. The Day 1 Hello Temporal Workflow, or any from earlier days, works.
- `temporal-testing` on the test classpath (added in Lab 4.2).

## Starter code

1. **Capture a history.** With a completed Workflow:

   ```bash
   temporal workflow show --workflow-id <id> --output json > src/test/resources/history.json
   ```

2. **Write the replay test:**

```java
// ReplayTest.java
class ReplayTest {
  @Test
  void replaysCleanly() throws Exception {
    WorkflowReplayer.replayWorkflowExecution(
        // TODO 1: load the history JSON from test resources
        //   (WorkflowReplayer.replayWorkflowExecution(History, Class<?>...)
        //    or the (File, Class<?>) / (String json, Class<?>) overload)
        // TODO 2: pass YOUR current workflow impl class so it replays against today's code
    );
    // No exception thrown == replay-compatible.
  }
}
```

## Tasks

1. Capture a history JSON from a real execution into `src/test/resources/`.
2. Write `replaysCleanly` so it replays that history against the current impl —
   it should pass.
3. **Break it on purpose.** Add a structural change to the Workflow that alters
   the command sequence (e.g. insert an extra Activity call *before* the existing
   one, or add a `Workflow.sleep`). Re-run — replay should now **fail** with a
   non-determinism error.
4. **Fix it correctly.** Gate the new behavior behind `Workflow.getVersion(...)`
   so old histories take the original path and only new executions take the new
   path. Re-run — replay passes again.

## Verification

```bash
mvn -q test -Dtest=ReplayTest
```

<details><summary>Under the hood — what <code>make run-replay</code> runs</summary>

```bash
cd examples/runnable/11-determinism-replay && mvn -q test
# Pure replay test — no Temporal server needed.
```

</details>

- Step 2: passes.
- Step 3: fails with a non-determinism / `NonDeterministicException`-style error
  pointing at the diverging event.
- Step 4: passes again, with new code guarded by a version check.

## Definition of done

- [ ] A captured production history replays clean against the original code.
- [ ] An un-gated structural change makes replay **fail** (you saw the error).
- [ ] The same change, gated with `Workflow.getVersion(...)`, replays clean.
- [ ] You can articulate why this test belongs in CI before every deploy.

## Pitfalls

- **What counts as a breaking change:** adding/removing/reordering Activity
  calls, timers, signals waited on, or child Workflows — anything that changes
  the *command* stream. Pure refactors that don't change commands are safe.
- `getVersion` must be called **unconditionally on the same code path** for a
  given change id; don't wrap the `getVersion` call itself in your new branch.
- Capture the history from a run that actually exercised the path you're
  changing, or the test won't cover the risk.

## Hints

<details><summary>Hint 1 — loading the history</summary>

`WorkflowHistory hist = WorkflowHistory.fromJson(Files.readString(path));` then
`WorkflowReplayer.replayWorkflowExecution(hist, YourWorkflowImpl.class);` — or
pass the file/JSON directly via the matching overload.
</details>

<details><summary>Hint 2 — the getVersion fix</summary>

```java
int v = Workflow.getVersion("add-pre-step", Workflow.DEFAULT_VERSION, 1);
if (v >= 1) {
  activities.newPreStep();   // only new executions
}
activities.original();
```
Old histories return `DEFAULT_VERSION` and skip the new step, so their command
stream is unchanged.
</details>

## Stretch goals

- Replace the `getVersion` approach with `@WorkflowVersioningBehavior(Pinned)` on
  the old impl and `AutoUpgrade` on the new one; discuss when each fits
  (short-lived vs. long-running Workflows).
- Add the replay test to a fixture of **several** captured histories and run them
  all — this is the realistic CI gate.
