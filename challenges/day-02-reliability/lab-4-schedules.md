# Lab 2.4 — Schedules

**Time:** ~35 min · **Difficulty:** ★★ · **Stack:** Temporal dev server

## Scenario

A daily sales report runs every morning at 09:00. In Airflow this is a
`schedule_interval` with `catchup` and a worry about overlapping runs. You'll
build the same thing as a Temporal **Schedule**, choosing an overlap policy on
purpose instead of inheriting Airflow's defaults.

## Learning goals

- Create a Schedule with the `ScheduleClient` Java API.
- Express a calendar spec (09:00 daily) and an overlap policy.
- Pause, trigger, and delete a Schedule from the CLI.
- Map Airflow `schedule_interval` / `catchup` to Temporal concepts.

> **Coming from Airflow `[airflow]`:** the Schedule is a first-class object you
> can describe, pause, and back-fill independently of the Workflow it launches.
> The overlap policy is the explicit answer to "what if the previous run hasn't
> finished?" — Airflow's `max_active_runs` + `catchup`, but clearer.

## Prerequisites

- Day 1 complete; `make temporal` running.

<details><summary>Under the hood — what <code>make temporal</code> runs</summary>

```bash
temporal server start-dev \
  --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234
```

gRPC on 127.0.0.1:7233, Web UI http://127.0.0.1:8233, metrics on :7234.
Overridable via env: `TEMPORAL_HOST`, `TEMPORAL_PORT`, `TEMPORAL_UI_PORT`, `TEMPORAL_METRICS_PORT`.

</details>

## Starter code

Scaffold a module in `training.temporal.schedules` (reuse Lab 1.2 `pom.xml`;
`artifactId` `schedules`, exec `mainClass`
`training.temporal.schedules.CreateSchedule`).

**Given contract** — the Workflow the Schedule will launch:

```java
// DailyReportWorkflow.java
package training.temporal.schedules;

import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface DailyReportWorkflow {
  @WorkflowMethod
  void run(String reportName);
}
```

Provide a trivial impl (log the report name; maybe `Workflow.sleep` a few
seconds to simulate work so you can observe overlap behavior).

**Schedule creator** — the core of the lab:

```java
// CreateSchedule.java
package training.temporal.schedules;

import io.temporal.client.schedules.*;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

public class CreateSchedule {
  public static void main(String[] args) {
    ScheduleClient scheduleClient =
        ScheduleClient.newInstance(WorkflowServiceStubs.newLocalServiceStubs());

    // TODO 1: build a ScheduleActionStartWorkflow that starts DailyReportWorkflow
    //         with an argument (e.g. "daily-sales") on task queue "reports",
    //         with a stable workflowId.
    // TODO 2: build a ScheduleSpec with a calendar entry for hour=9, minute=0.
    // TODO 3: set a SchedulePolicy overlap of your choice (start with SKIP).
    // TODO 4: assemble Schedule.newBuilder()...build() and
    //         scheduleClient.createSchedule(<id>, schedule, ScheduleOptions...).
  }
}
```

You'll also need a Worker polling the `reports` Task Queue and registering
`DailyReportWorkflowImpl` so scheduled runs actually execute. (Reuse the
Worker pattern from earlier labs, or run `make`-style in a second `main`.)

## Tasks

1. Build the action, spec (09:00 daily), and policy; create the Schedule.
2. Start a Worker on `reports` so scheduled Workflows run.
3. From the CLI, confirm the Schedule exists, then **trigger** an immediate run
   rather than waiting until 09:00.
4. Change the overlap policy and re-create the Schedule; reason about the
   difference using a Workflow that sleeps longer than the trigger interval.

## Verification

```bash
temporal schedule list
temporal schedule describe --schedule-id daily-sales-report-schedule

# Fire one run now instead of waiting for the calendar time
temporal schedule trigger  --schedule-id daily-sales-report-schedule
temporal workflow list      # the triggered DailyReportWorkflow run appears

# Pause / unpause / clean up
temporal schedule toggle   --schedule-id daily-sales-report-schedule --pause --reason "lab"
temporal schedule delete   --schedule-id daily-sales-report-schedule
```

## Definition of done

- [ ] A Schedule exists with a 09:00-daily calendar spec.
- [ ] Triggering it launches a `DailyReportWorkflow` run you can see in the UI.
- [ ] You chose an overlap policy deliberately and can explain it.
- [ ] You can map `schedule_interval` and `catchup` to their Temporal analogues.

## Overlap policy cheat-sheet

| Policy | Behavior when a run is still going | Airflow analogue |
|---|---|---|
| `SKIP` | Skip the new run | `max_active_runs=1`, no catchup |
| `BUFFER_ONE` | Queue exactly one to run next | serialized, one pending |
| `ALLOW_ALL` | Start it anyway, run concurrently | `max_active_runs` > 1 |
| `TERMINATE` | Kill the running one, start fresh | (no clean analogue) |

## Hints

<details><summary>Hint 1 — calendar spec for 09:00</summary>

Use `ScheduleCalendarSpec.newBuilder().setHour(List.of(new ScheduleRange(9)))
.setMinutes(List.of(new ScheduleRange(0)))`. Wrap it in
`ScheduleSpec.newBuilder().setCalendars(List.of(...))`.
</details>

<details><summary>Hint 2 — overlap policy enum</summary>

`SchedulePolicy.newBuilder().setOverlap(
ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)`. Swap the enum value to
experiment.
</details>

<details><summary>Hint 3 — testing overlap without waiting all day</summary>

Switch the spec to an interval of a few seconds and make the Workflow sleep
longer than that interval, then watch how many concurrent runs appear under each
policy.
</details>

## Stretch goals

- Add **jitter** to the spec and observe start-time spread across several runs.
- Set a `catchup` window (backfill) and use `temporal schedule backfill` to
  simulate Airflow catchup for a missed range. Decide whether you actually want
  catchup for this report.
