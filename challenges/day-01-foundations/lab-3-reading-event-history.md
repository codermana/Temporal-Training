# Lab 1.3 — Reading the Event History

**Time:** ~30 min · **Difficulty:** ★ · **Stack:** Temporal dev server

## Scenario

Temporal's durability comes from one thing: an append-only **Event History** per
Workflow execution. Replay re-runs your Workflow code against that history to
reconstruct state. If you can read a history, you can debug anything. This lab
turns the Hello Temporal run from Lab 1.2 into a thing you can read.

## Learning goals

- Dump a Workflow's Event History from the CLI and the Web UI.
- Identify the command/event pairs that drive an Activity call.
- Explain what "deterministic replay" actually reconstructs.

> **Coming from Airflow `[airflow]`:** there is no XCom table and no task-state
> DB to cross-reference. The history *is* the source of truth — inputs, outputs,
> timers, retries, and signals, all in one ordered log.

## Prerequisites

- Lab 1.2 produced at least one **Completed** `GreetingWorkflow` execution.
- `make temporal` running.

## Tasks

1. **List executions and grab an ID.**

   ```bash
   temporal workflow list
   ```

   Note the **Workflow ID** of your Hello Temporal run.

2. **Dump the full history as JSON.**

   ```bash
   temporal workflow show --workflow-id <your-workflow-id> --output json
   ```

   Scroll through the events. Find and label, in order:
   - `WorkflowExecutionStarted` — the input you passed.
   - `WorkflowTaskScheduled` / `Started` / `Completed` — the Worker deciding
     what to do next.
   - `ActivityTaskScheduled` / `Started` / `Completed` — the Activity call and
     its **result**.
   - `WorkflowExecutionCompleted` — the final return value.

3. **Find the inputs and outputs.** Locate the `name` argument inside the
   `WorkflowExecutionStarted` event and the greeting string inside the
   `ActivityTaskCompleted` event. Confirm they match what your code did.

4. **Read the same thing in the Web UI.** Open the execution at
   <http://127.0.0.1:8233>, switch between the **Compact** and **History**
   views, and expand an event to see its payload.

5. **Reason about replay.** Answer for yourself: if the Worker crashed right
   after `ActivityTaskCompleted` but before `WorkflowExecutionCompleted`, what
   would Temporal replay on restart, and would the Activity run a second time?

## Verification

You can answer all of these from the history alone:

- What value was passed into the Workflow?
- What did the Activity return?
- How many Workflow Tasks did this execution need?

## Definition of done

- [ ] You dumped the history via CLI **and** viewed it in the Web UI.
- [ ] You can point to the event holding the Activity's result.
- [ ] You can explain, in one sentence, what replay reconstructs and why the
      completed Activity is **not** re-executed.

## Hints

<details><summary>Hint 1 — too much JSON</summary>

Pipe through a pager or `jq`. To see just event types in order:

```bash
temporal workflow show --workflow-id <id> --output json \
  | jq -r '.events[].eventType'
```
</details>

<details><summary>Hint 2 — where's the result?</summary>

Activity results live in the `activityTaskCompletedEventAttributes.result`
payload; the Workflow's final result lives in
`workflowExecutionCompletedEventAttributes.result`. The Web UI decodes these for
you under each event's payload section.
</details>

<details><summary>Hint 3 — replay answer</summary>

Replay feeds the recorded history back through your Workflow code. Because
`ActivityTaskCompleted` is already in the log, the SDK returns the recorded
result instead of scheduling the Activity again — so it does **not** run twice.
Only the un-recorded tail (the final completion) is produced fresh.
</details>

## Stretch goals

- Run the Workflow again with a different name and diff the two histories. Which
  events change, which stay structurally identical?
- Use `temporal workflow show --workflow-id <id> --output json > history.json`
  and keep the file — Day 4's replay-testing lab reuses exactly this artifact.
- Force a retry (throw once from the Activity, as in Lab 1.2's stretch) and find
  the `ActivityTaskFailed` event plus the retry's second `ActivityTaskScheduled`.
