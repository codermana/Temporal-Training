# Lab 2.3: Updates

**Time:** ~40 min · **Difficulty:** ★★ · **Stack:** Temporal dev server

## Scenario

Signals are fire-and-forget; Queries are read-only. Sometimes you need to send
data **into** a running Workflow *and get a response back*, and reject bad input
before it's ever recorded. That's the Update API. You'll extend the approval
Workflow from Lab 2.2 so a caller can amend the order's note and get the new
state synchronously, with validation.

## Learning goals

- Implement `@UpdateMethod` for synchronous request/response into a Workflow.
- Reject invalid input early with `@UpdateValidatorMethod`.
- Understand `WorkflowUpdateStage` and why `setWaitForStage(...)` is required.

> **Coming from Airflow `[airflow]`:** there's no equivalent; DAGs can't accept
> a validated, acknowledged mutation mid-run. This is closer to calling a method
> on a long-lived actor and awaiting its return value.

## Prerequisites

- **Lab 2.2 complete**: same module and `ApprovalWorkflow`. The interface
  already declares `changeNote` and `validateNote`.

## Starter code

Continue in the Lab 2.2 module. Replace the two stubbed methods in
`ApprovalWorkflowImpl`:

```java
  @Override
  public String changeNote(String note) {
    // TODO: update the note field and return the new currentState().
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public void validateNote(String note) {
    // TODO: reject null/blank notes by throwing. A thrown exception here means
    //       the update is rejected and never recorded in history.
    throw new UnsupportedOperationException("TODO");
  }
```

The `@UpdateValidatorMethod(updateName = "changeNote")` annotation on the
interface already binds this validator to the `changeNote` update.

<details><summary><b>Doing this lab in Python or Go?</b> Starter scaffolds</summary>

An Update is a request/response handler with an optional **validator**. A
validator that raises rejects the update *before* it is written to history.

**Python** (`temporalio`): `@workflow.update` with a paired validator:

```python
    @workflow.update
    def change_note(self, note: str) -> str:
        self._note = note
        return self.current_state()

    @change_note.validator
    def validate_note(self, note: str) -> None:
        # raise to reject: nothing is recorded in history
        if not note or not note.strip():
            raise ValueError("note must not be blank")
```

**Go** (`go.temporal.io/sdk`): register a handler with a validator option:

```go
    err := workflow.SetUpdateHandlerWithOptions(ctx, "changeNote",
        func(ctx workflow.Context, note string) (string, error) {
            noteVar = note
            return status, nil
        },
        workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, note string) error {
            if strings.TrimSpace(note) == "" {
                return errors.New("note must not be blank") // rejected, not recorded
            }
            return nil
        }},
    )
```

The CLI (`temporal workflow update execute --name changeNote ...`) is the same
across SDKs, including the rejection behavior you verify below.

</details>

## Tasks

1. Implement `validateNote` to throw `IllegalArgumentException` on a null/blank
   note.
2. Implement `changeNote` to set the note and return `currentState()`.
3. Restart the Worker (the `approval-demo` Workflow should be `WAITING`).
4. Send a valid update from the CLI and read the returned value.
5. Send an **invalid** (empty) update and confirm it's rejected, and that the
   rejection left **no** event in history.

## Verification

```bash
# Valid update: returns the new state synchronously
temporal workflow update execute --workflow-id approval-demo --name changeNote \
  --input '"expedite before close of business"'

# Invalid update: rejected by the validator
temporal workflow update execute --workflow-id approval-demo --name changeNote \
  --input '""'

# Confirm: the accepted update is in history; the rejected one is not
temporal workflow show --workflow-id approval-demo --output json \
  | jq -r '.events[].eventType' | grep -i update
```

Expected: the valid update prints the updated state; the empty update errors
with your validation message; history shows `WorkflowExecutionUpdateAccepted`/
`Completed` for the valid one and **nothing** for the rejected one.

> CLI note: older CLIs use `temporal workflow update --name ...`; current ones
> use `temporal workflow update execute --name ...`. Use whichever your
> `temporal workflow update --help` shows.

## Definition of done

- [ ] A valid `changeNote` update returns the new state to the caller.
- [ ] A blank note is rejected by the validator and produces no history event.
- [ ] You can explain the difference between a Signal and an Update.

## Programmatic stretch: `startUpdate` and `startUpdateWithStart`

From a client (not the Workflow), the SDK requires you to pass an explicit stage
via `UpdateOptions.setWaitForStage(WorkflowUpdateStage....)`:

- `ACCEPTED`: return once the update passes validation and is durably accepted.
- `COMPLETED`: block until the update handler returns its result.

Try, in a small client `main`:

1. `WorkflowClient.startUpdate(...)` with `WaitForStage(ACCEPTED)` and fetch the
   result later via the returned handle, useful when you don't want to block.
2. `WorkflowClient.startUpdateWithStart(...)` with a `WithStartWorkflowOperation`
   to **atomically** start the Workflow and apply the first update in one call.

## Hints

<details><summary>Hint 1: validator semantics</summary>

The validator runs **before** the update is admitted. If it throws, the caller
gets the error and nothing is appended to history. Keep it pure, same rules as
a Query: no Activities, no `await`, no mutation.
</details>

<details><summary>Hint 2: why setWaitForStage is mandatory</summary>

There's no safe default: returning at `ACCEPTED` vs `COMPLETED` changes the
caller's consistency guarantees, so the SDK forces you to choose. Omitting it is
a programming error, not a silently-defaulted option.
</details>

## Pitfalls

- A validator that mutates state will corrupt replay; keep it read-only.
- Don't put long-running work in the validator; it must be fast and
  deterministic.
