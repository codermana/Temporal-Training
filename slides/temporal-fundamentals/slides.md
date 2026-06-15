---
marp: true
theme: base
paginate: true
size: 16:9
transition: fade 0.4s
title: Temporal Fundamentals
description: A Java-first 24-hour Temporal training mapped to the course Agenda.
author: Gaurav Agarwal
footer: "![CoderMana](assets/codermana.svg)"
---

<!-- _class: title -->
<!-- _transition: coverflow 0.7s -->

###### Fundamentals

# Temporal Fundamentals

###### Gaurav Agarwal

<!--
6 days × 4 hours.

Each day mirrors a day in lecture_notes/Day-XX.md.

Lab slides are marked - laptops out, fingers on keyboards.

Pace check: end of Day 1 should leave the room with one Workflow running.
-->

---

# Course Agenda

| Day | Topic | Lab focus |
| --- | --- | --- |
| 1 | Foundations - durable execution mental model | Hello Temporal, Event History |
| 2 | Reliability + interactions | Signals, Updates, Schedules |
| 3 | Kafka integration | End-to-end Kafka pipeline, DLQ |
| 4 | Production engineering | Replay tests, dashboards |
| 5 | Saga + Spring Boot + capstone | Capstone Workflow |
| 6 | AWS migration + containers | Glue, K8s, KEDA |

Build-it-yourself labs (starter code, no solutions) live in [`challenges/`](https://github.com/codermana/Temporal-Training/tree/master/challenges) — each lab slide links its own challenge.

<!--
Quick orientation slide.

Don't dwell - each Day cover slide opens the detailed agenda for that block.
-->

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 1

# Foundations

Rethinking orchestration as durable application code.

<!--
4 hours. Morning interleaves concepts with the first hands-on labs;
afternoon goes deeper on the event history.

Get everyone's environment green before teaching anything else.
-->

---

<!-- _class: lab -->

###### Lab · Day 1

# Local dev setup

Challenge → [`day-01-foundations/lab-1-local-dev-setup`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-01-foundations/lab-1-local-dev-setup.md)

```bash
make check          # verify required tools
make temporal       # start dev server (in this terminal)

# in another terminal
open http://127.0.0.1:8233
temporal operator namespace list
```

> Goal: every laptop shows the `default` namespace in the Web UI.

<!--
Wait until every laptop is green.

Pair the stragglers.

Don't proceed without this; the rest of the day depends on it.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 1

# Why Temporal exists

The failure modes of cron- and DAG-based orchestration.

<!--
Open in VSCode: examples/01-foundations/airflow_dag_vs_temporal_workflow.java + .py - DAG shape vs durable code, side by side.
-->


---

# Every backend has these

* "Charge the card, ship the order, send the receipt."
* "Pull from S3, transform with Spark, write to Snowflake."
* "Wait for the human approval, then provision the tenant."
* "Retry the flaky API for an hour, then page the on-call."

These are **workflows**. They look easy until one step fails.

<!--
Read in different voices.

Each shape will resonate with someone in the room.
-->

---

# What goes wrong

* The third call timed out. Did it succeed?
* The Lambda was killed at minute 14 of 15.
* The Kafka consumer crashed *between* the DB write and the publish.
* The cron didn't fire. Nobody noticed for two days.
* The retry loop never had a budget.

> Recovery is a **runbook**, not a button.

<!--
The slogan to repeat across the day: runbook, not a button.
-->

---

<!-- _class: dense -->

# Tools you've shipped with

| Stack | What it solves | What it leaves to you |
| --- | --- | --- |
| Cron + scripts | Triggering on a schedule | All state, retries, recovery |
| Airflow | Scheduling DAGs | Cross-system state, retries on top |
| Step Functions | State machines in JSON | Code review, Lambda 15-min cap |
| Kafka alone | Transport between systems | Per-key state, idempotency |

Drift across all of them = the 2 AM page.

<!--
Don't bash any tool.

Each solves a real problem.

The point is the seam each leaves open, not that any is bad.
-->

---

<!-- _class: dense -->

# DAGs vs durable execution

| | Apache Airflow | Temporal |
| --- | --- | --- |
| Paradigm | A **DAG** of tasks you wire up | Idiomatic code that runs top-to-bottom |
| Trigger | **Schedule-driven** — hourly, midnight, quarter-end | **Event-driven** — API call, webhook, message |
| Recovery | Retry a task by its position in the graph | Replay history; resume mid-function |
| Sweet spot | Move + transform data on a schedule | Coordinate microservices & business logic |

> Airflow moves data A→B. Temporal runs a **durable function** that survives crashes.

<!--
This is the paradigm shift, stated once, early. Everything on Day 1 builds on
"durable function," not "graph of tasks."

Airflow is schedule-first and polls; Temporal reacts to events with sub-second
latency - that's why it fits user-facing flows Airflow can't serve.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 1

# Core concepts

Workflows, Activities, Workers, Task Queues.

<!--
Open in VSCode: examples/01-foundations/core_primitives.java - all four roles in one file.
Run: make run-hello
-->


---

<!-- _class: cards -->

# The four primitives

| Workflow | Activity | Worker | Task Queue |
| --- | --- | --- | --- |
| Durable function. State is the event history. Deterministic. | Arbitrary code with side effects. Retried independently. | Long-lived process polling one or more Task Queues. | A string name. Routes work to a Worker pool. |

<!--
Four cards, four primitives.

Task Queue is JUST A STRING.

Not Kafka.

Not a DB.

It's a routing key.
-->

---

# Workflow

```java
@WorkflowInterface
public interface OrdersWorkflow {
  @WorkflowMethod
  void run(String batchDate);
}
```

- Annotated Java interface + impl.
- The impl is your **durable function**.
- State lives in event history; the impl is replayable.

<!--
This is just a Java interface.

The SDK uses the @WorkflowInterface annotation to identify it via reflection.
-->

---

# Activity

```java
@ActivityInterface
public interface OrdersActivities {
  String extract(String batchDate);
  String transform(String rawUri);
  void load(String cleanUri);
}
```

- Unrestricted code: HTTP, DB, files, anything.
- Retried independently of the Workflow.
- 95% of your real production code lives here.

---

# Worker + Task Queue

```java
WorkerFactory factory = WorkerFactory.newInstance(client);
Worker worker = factory.newWorker("orders");
worker.registerWorkflowImplementationTypes(OrdersWorkflowImpl.class);
worker.registerActivitiesImplementations(new OrdersActivitiesImpl());
factory.start();
```

- Worker = long-lived JVM polling `orders` Task Queue.
- The Task Queue **string** routes work to a Worker pool.

<!--
factory.start() kicks off the long-poll loop.

Workers connect outbound.
-->

---

<!-- _class: dense -->

# Airflow → Temporal map

| Airflow | Temporal |
| --- | --- |
| DAG | Workflow |
| Operator / Task | Activity |
| Worker | Worker (long-lived JVM) |
| `default_queue` | Task Queue |
| XCom | A normal Java return value |
| `ExternalTaskSensor` | Signal / `signalWithStart` |
| `BranchPythonOperator` | `if` / `switch` in Java |
| Sensor poll loop | `Workflow.await(predicate)` |

> You stop describing shape. You start writing behavior.

<!--
For Airflow rooms, this slide is the moment of recognition.

XCom-becomes-a-return-value gets the biggest reaction.
-->

---

<!-- _class: dense -->

# At a glance

| Feature | Apache Airflow | Temporal |
| --- | --- | --- |
| Primary domain | Data engineering & batch | App development & microservices |
| State management | Central metadata DB of task status | Event-sourced history, replayed |
| Latency | High — polling, seconds to start | Low — gRPC, sub-second |
| Waiting / sleep | Costs a worker slot or a sensor | Native & cheap — sleep for a year |
| Scaling limit | Scheduler + metadata DB | Workflow history size |

> Not better-or-worse — different problems. Match the tool to the shape of the work.

<!--
The scaling-limit row is the honest one: Temporal isn't free of limits, it just
moves them. History size is the constraint you design around (continue-as-new on
Day 5).

Pair this with the migration framework on Day 4 - "migrate where Temporal earns
its keep."
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 1

# Event sourcing & deterministic replay

The single concept that breaks the most Airflow brains.

<!--
Open in VSCode: examples/01-foundations/deterministic_replay_bad.java vs deterministic_replay_good.java - diff them side by side.
-->


---

# The replay rule

When a Worker resumes a Workflow:

1) It re-runs the Workflow code from the start.
2) Replays recorded events to reconstruct local state.
3) Reaches the next undecided point.
4) Continues from there.

> Different decision than the recorded history = non-determinism error.

<!--
Whiteboard moment.

Walk through with arrows.

"Replay" doesn't re-execute side effects - Activity results are READ from history.
-->

---

<!-- _class: dense -->

# Five families of non-determinism

```java
// 1. Time
long now = System.currentTimeMillis();         // NO
long now = Workflow.currentTimeMillis();       // YES

// 2. Random
int n = new Random().nextInt(10);              // NO
int n = Workflow.newRandom().nextInt(10);      // YES

// 3. I/O
Files.writeString(path, "x");                  // NO - move to Activity

// 4. Concurrency
Thread.sleep(60_000);                           // NO
Workflow.sleep(Duration.ofMinutes(1));         // YES

// 5. Iteration order
for (var e : hashMap.entrySet()) { ... }       // risky
```

<!--
Reference card. The throughline: anything whose value the Worker can't reproduce
on replay must be sourced from history, not recomputed. Walk each family:

1. TIME - System.currentTimeMillis() returns a new value every replay. The first
   run records 10:00:00; a replay tomorrow recomputes 10:00:01 and the code
   branches differently. Workflow.currentTimeMillis() returns the value recorded
   in history, so every replay sees the same instant.

2. RANDOM - same trap. new Random() reseeds from the system clock; replay gets a
   different number. Workflow.newRandom() seeds deterministically from the run and
   records the seed, so the sequence is reproducible. (Use this for jitter, IDs,
   A/B bucketing - not java.util.Random.)

3. I/O - reading a file, calling an HTTP endpoint, or hitting a DB gives a
   different answer each replay AND fires the side effect twice. There is no
   Workflow.* substitute - the fix is to MOVE it into an Activity. Activities run
   once and their result is recorded; replay reads the result from history.

4. CONCURRENCY - the biggest aha. Thread.sleep blocks a Worker thread for the full
   duration; Workflow.sleep records a timer and the Worker FORGETS the Workflow
   entirely (lead-in to the next slide). Same rule for threads/locks: use
   Workflow.newThread / Async / Workflow primitives, never raw java.lang.Thread,
   so the SDK controls scheduling deterministically.

5. ITERATION ORDER - HashMap/HashSet have no guaranteed order, and it can differ
   across JVM versions or runs. If you iterate one to make a decision (pick first,
   sum in order, branch on order) replay can diverge. Fix: use a TreeMap /
   LinkedHashMap or sort the keys before iterating. "risky," not "always wrong" -
   it only breaks if order affects a recorded decision.

Tie it back to the replay rule slide: every one of these makes the re-run reach a
DIFFERENT decision than history = non-determinism error, Workflow stuck/failed.
-->

---

# Durable sleep

```java
Workflow.sleep(Duration.ofDays(30));
```

- The Worker *forgets* this Workflow.
- The server fires a timer in 30 days.
- Some Worker - maybe a different one - picks it up and continues.

> No JVM stays alive. Survives every deploy in between.

<!--
Ask: "how would you wait 30 days for an email opt-in today?" Compare to one line.
-->

---

<!-- _class: lab -->

###### Lab · Day 1

# Hello Temporal

Challenge → [`day-01-foundations/lab-2-hello-temporal`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-01-foundations/lab-2-hello-temporal.md)

```bash
make run-hello           # terminal 1: the Worker (stays up, polls the queue)
make run-hello-starter   # terminal 2: the starter — starts one Workflow
```

Worker and starter are **separate processes**, as in production — they share only the Task Queue name. Order doesn't matter: start the Workflow first and the server holds it on the queue until a Worker polls.

<!--
This is the real production topology, not a toy wiring: the Worker is a
long-lived deployment; the starter is whatever fires work - an HTTP handler, a
cron, a CLI. Both only ever talk to the server.
-->

---

<!-- _class: lab -->

###### Lab · Day 1

# Hello Temporal — read the history

1. Workflow appears in the Web UI under `default` namespace.
2. Click into it; open the Event History tab.
3. Identify `WorkflowExecutionStarted` and the `ActivityTask*` events.

> Restart the Worker mid-run; the Workflow resumes. That's the lesson.

<!--
Have one person KILL the Worker mid-run on purpose.

The Workflow completes when the Worker restarts.

This is the most important moment of Day 1.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 1

# Architecture

What's inside the box.

<!--
Run: make run-hello, then read the Web UI event history. Dump it from the CLI with examples/01-foundations/history_cli.sh.
-->


---

<!-- _class: code -->

## The cluster

```
                    ┌──────────────┐
   SDK / CLI  ───▶ │   Frontend   │   gRPC API
                    └──────┬───────┘
                    ┌──────▼───────┐
                    │   History    │   workflow state machine
                    └──────┬───────┘
                    ┌──────▼───────┐
                    │   Matching   │   Task Queue dispatch
                    └──────┬───────┘
                    ┌──────▼───────┐
                    │ Persistence  │   PostgreSQL / Cassandra
                    └──────────────┘
```

Your Workers connect **outbound** to Frontend on `:7233`.

<!--
Simplified mental model first - the next slide shows the real topology.

Trace one Workflow start: SDK → Frontend → History (write
WorkflowExecutionStarted) → Matching → Worker polls.
-->

---

<!-- _class: image image-credit -->

## The cluster: four services + persistence

![High-level Temporal architecture](assets/temporal-high-level.svg)

Temporal Technologies — [temporal/docs/architecture](https://github.com/temporalio/temporal/blob/main/docs/architecture/README.md)

<!--
Frontend is a gateway in front of three peer services; History and Matching both
own persistence. Your Workers live OUTSIDE this box and connect outbound to
Frontend on :7233. Keep the source credit on the slide.
-->

---

<!-- _class: cards -->

# The four services

| Frontend | History | Matching | Worker |
| --- | --- | --- | --- |
| Stateless gRPC gateway. Auth, rate-limiting, routing, request validation. Every SDK/CLI call lands here. | Owns Workflow Execution state. Writes the event history, runs the state machine, enqueues tasks. Sharded. | Hosts Task Queues. Matches tasks from History to Workers polling by queue name. | Internal background service: replication, archival, schedules, batch ops, cleanup. **Not** your Worker. |

> Your application Worker is a *client* of this cluster, not part of it.

<!--
The naming trap: "Worker Service" inside the cluster is internal background work.
The Worker YOU write and deploy is a separate process polling Matching via Frontend.
-->

---

<!-- _class: dense -->

# History Service & shards

- Workflow state is partitioned into **shards** (e.g. 512 / 4096); each shard owns a slice of executions by hashed Workflow ID.
- A shard is owned by exactly **one** History host at a time → single-writer, no contention per workflow.
- Each shard drives its executions and processes internal **task queues**:

| Internal queue | Drives |
| --- | --- |
| Transfer tasks | Push Workflow/Activity tasks to Matching; start child workflows |
| Timer tasks | Fire durable timers, `Workflow.sleep`, timeouts, retries |
| Visibility tasks | Update the searchable/visibility store |
| Replication tasks | Ship events to other clusters (multi-cluster) |

> Scaling History = more shards spread across more History hosts.

---

<!-- _class: dense -->

# Three task types

| Task | Worker does | Result |
| --- | --- | --- |
| **Workflow Task** | Resume Workflow code until it blocks or completes | Commands back to History (schedule activity, start timer, complete) |
| **Activity Task** | Execute your Activity code (side effects allowed) | Success/failure reported to History |
| **Query Task** | Run a read-only query over current state | Value returned; history **not** advanced |

> History produces tasks; Matching dispatches them; your Worker pulls and runs them.

---

<!-- _class: code -->

## Lifecycle: one Workflow start

```
1. Client ──StartWorkflowExecution──▶ Frontend ──▶ History (owning shard)
2. History  appends WorkflowExecutionStarted + WorkflowTaskScheduled
            └─ transfer task ──▶ Matching   (enqueue on Task Queue)
3. Worker   long-polls Task Queue via Frontend ──▶ gets Workflow Task
4. Worker   runs code, returns command: ScheduleActivityTask ──▶ History
5. History  ──transfer task──▶ Matching ──▶ Worker gets Activity Task
6. Worker   runs Activity, reports result ──▶ History (appends events)
7. History  schedules next Workflow Task … repeat until completion
```

Everything durable is an **event appended by History** before any Worker sees it.

<!--
Walk this slowly on the whiteboard. The key insight: nothing the Worker does is
trusted until History has written the resulting event. Crash anywhere and replay
rebuilds from the persisted history.
-->

---

# What's on your laptop

`temporal server start-dev` bundles:

- Frontend + History + Matching + internal Worker
- PostgreSQL (or in-memory)
- Web UI on `:8233`
- Prometheus metrics on `:7234` with `--metrics-port`

> One binary today. Same gRPC contract as production.

---

<!-- _class: lab -->

###### Lab · Day 1 · optional

# Hello Temporal on Docker

Challenge → [`day-01-foundations/lab-2-hello-temporal-docker`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-01-foundations/lab-2-hello-temporal-docker.md)

Same Workflow, real cluster. Only the **environment** changes:

```bash
make stack-temporal       # auto-setup + Postgres + UI on :7233 / :8233
make run-connect          # terminal 1: env-driven Worker
make run-connect-starter  # terminal 2: start one Workflow
```

- Single binary → **four services + PostgreSQL**, each its own container.
- No env, no creds → the **plaintext** branch defaults to `127.0.0.1:7233`.
- State now survives restarts — **durable Postgres**, not in-memory.

<!--
Connections.fromEnv() is the shared base; Cloud (next) feeds it credentials and
takes the TLS branch. Worker and starter are separate processes, both reading
the same env.

Mirror of the Cloud slide and built on the SAME worker - here the plaintext
branch, Cloud the TLS branch. Stop 'make temporal' first; the stack binds :7233.
Good moment to restart the temporal container and show the Workflow survived.
-->

---

<!-- _class: lab -->

###### Lab · Day 1 · optional

# Hello Temporal on the Cloud

Challenge → [`day-01-foundations/lab-2-hello-temporal-cloud`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-01-foundations/lab-2-hello-temporal-cloud.md)

Same worker — Cloud creds make `Connections.fromEnv()` take the TLS branch:

```bash
export TEMPORAL_ADDRESS=...:7233   TEMPORAL_NAMESPACE=my-ns.acct
export TEMPORAL_API_KEY=...        # or TEMPORAL_TLS_CERT / _KEY
make run-connect           # Worker (terminal 1)
make run-connect-starter   # starter (terminal 2)
```

- Set the **namespace** explicitly (`my-ns.acct`), not `default`.
- Execution lands in the **Cloud** UI, not your laptop.

<!--
Punchline to say out loud: laptop -> Docker -> Cloud is all env, not a rewrite.

Optional - demo-only if attendees have no Cloud creds. Same shared module as the
Docker slide (examples/runnable/01b-hello-temporal-anywhere); with no creds it
falls back to plaintext, so it still compiles and runs against make temporal.
-->

---

<!-- _class: lab -->

###### Lab · Day 1

# Read the Event History

Challenge → [`day-01-foundations/lab-3-reading-event-history`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-01-foundations/lab-3-reading-event-history.md)

```bash
temporal workflow show \
  --workflow-id hello-temporal-demo --output json \
  | jq '.events[].eventType'
```

Identify in order:

- `WorkflowTaskScheduled` / `Started` / `Completed`  - the decision loop
- `ActivityTaskScheduled` / `Started` / `Completed`  - the work loop
- `WorkflowExecutionCompleted` - final outcome

<!--
This grep-able view is the production debugging starting point.

Show it now; it'll come back on Day 4 for replay tests.
-->

---

<!-- _class: takeaway -->

# Day 1 takeaways

* One model: **Workflow code re-executes on replay; Activity results are recorded.**
* One discipline: keep Workflow code deterministic; do all I/O in Activities.
* One habit: pick Workflow IDs from business identity. They're durable handles.

<!--
The first slogan to repeat.

If only one thing sticks for Day 1, it's the re-execution-vs-recorded-results
distinction.
-->

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 2

# Building reliable Workflows

Async, retries, heartbeats - and the ways you interact with running executions.

<!--
4 hours: morning is reliability mechanics; afternoon is signals/queries/
updates/schedules/children.

Lots of code.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Async and parallel Activity execution

Promises, not threads.

<!--
Open in VSCode: examples/02-reliability/async_activity.java, parallel_fanout_allof.java
Run: make run-async
-->


---

# Sequential vs Async

```java
// Sequential - one Activity at a time
String rawUri = activities.extract(batchDate);
String cleanUri = activities.transform(rawUri);

// Async - two extracts run in parallel
Promise<String> rawUri   = Async.function(activities::extract, batchDate);
Promise<String> auditUri = Async.function(activities::extract, batchDate + "-audit");
String cleanUri      = activities.transform(rawUri.get());
String cleanAuditUri = activities.transform(auditUri.get());
```

> `Promise.get()` blocks the *Workflow loop*, not an OS thread.

---

<!-- _class: code -->

## Fan-out / fan-in

```java
List<Promise<Integer>> counts =
    partitions.stream()
        .map(p -> Async.function(activities::processPartition, p))
        .toList();

Promise.allOf(counts).get();
int total = counts.stream().mapToInt(Promise::get).sum();
```

All partitions run in parallel. The Workflow suspends across all of them.

<!--
One JVM hosts tens of thousands of suspended Workflows.

Each one is just heap state, not a parked thread.
-->

---

<!-- _class: code -->

## Async.procedure & first-to-finish

```java
// void Activities use Async.procedure (Async.function is for return values)
List<Promise<Void>> sends =
    userIds.stream().map(id -> Async.procedure(notify::send, id)).toList();
Promise.allOf(sends).get();

// race two providers; continue when the FIRST returns
Promise<String> primary  = Async.function(notify::askPrimary, q);
Promise<String> fallback = Async.function(notify::askFallback, q);
Promise.anyOf(primary, fallback).get();
```

> `allOf` waits for every branch; `anyOf` wakes on the first.

<!--
Example: examples/02-reliability/async_procedure_and_race.java
-->

---

<!-- _class: code -->

## Partial failure in a fan-out

```java
Map<Integer, Promise<String>> futures = new LinkedHashMap<>();
for (int p : partitions)
  futures.put(p, Async.function(activities::processWithStatus, p));

Map<Integer, String> result = new LinkedHashMap<>();
for (var e : futures.entrySet()) {
  try { result.put(e.getKey(), e.getValue().get()); }
  catch (ActivityFailure failure) {
    result.put(e.getKey(), "FAILED: " + failure.getMessage());
  }
}
```

> One branch failing doesn't sink the others - collect per-branch outcomes.

<!--
Example: examples/02-reliability/partial_failure.java
-->

---

<!-- _class: lab -->

###### Lab · Day 2

# Async + parallel activities

Challenge → [`day-02-reliability/lab-1-async-parallel-activities`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-02-reliability/lab-1-async-parallel-activities.md)

```bash
make run-async           # terminal 1: the Worker
make run-async-starter   # terminal 2: start one Workflow
```

In the Web UI:

1. Note that all `ActivityTaskScheduled` events appear with the *same* timestamp.
2. Compare to a sequential variant: events stagger.
3. Pair: predict what happens if one of three parallel Activities fails.

<!--
Have students sketch on paper before running.

Then run and verify their prediction was right (or wrong - even better).
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Retries, timeouts, heartbeats

Know what each setting controls or you'll misuse all of them.

<!--
Open in VSCode: examples/02-reliability/retry_and_timeouts.java, heartbeat_long_activity.java
-->


---

<!-- _class: dense -->

# Three timeouts

| Setting | Controls |
| --- | --- |
| `startToCloseTimeout` | One attempt's wall-clock budget |
| `scheduleToCloseTimeout` | Total budget across **all** retry attempts |
| `scheduleToStartTimeout` | How long an Activity sits in the queue before pickup |
| `heartbeatTimeout` | Max gap between heartbeats; Worker death detect |

> If you can't say *why* a timeout is 5 minutes, it's wrong.

---

<!-- _class: code dense -->

## Setting them deliberately

```java
ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofMinutes(5))
    .setScheduleToCloseTimeout(Duration.ofMinutes(30))
    .setHeartbeatTimeout(Duration.ofSeconds(30))
    .setRetryOptions(
        RetryOptions.newBuilder()
            .setInitialInterval(Duration.ofSeconds(5))
            .setBackoffCoefficient(2.0)
            .setMaximumInterval(Duration.ofMinutes(1))
            .setMaximumAttempts(6)
            .build())
    .build();
```

6 × 5min attempts + 6 backoff waits ≈ 33min — set scheduleToClose to bound it.

<!--
The arithmetic is the lesson.

Bring a calculator if you don't trust the audience to do it on paper.
-->

---

<!-- _class: code -->

## Heartbeats

<!-- Open in VSCode: examples/02-reliability/heartbeat_long_activity.java -->

```java
public String exportLargeTable(String tableName) {
  for (int page = 0; page < 1000; page++) {
    try {
      exportPage(tableName, page);
      Activity.getExecutionContext().heartbeat(page);
    } catch (ActivityCanceledException | ActivityPausedException stop) {
      cleanupPartialExport(tableName, page); throw stop;
    }
  }
  return "s3://exports/" + tableName;
}
```

> On retry, read the last heartbeat detail and *resume from page N*.

---

<!-- _class: code -->

## CancellationScope - race against a deadline

```java
CompletablePromise<String> result = Workflow.newPromise();

CancellationScope scope = Workflow.newCancellationScope(
    () -> result.completeFrom(Async.function(exports::exportLargeTable, table)));
scope.run();

if (!Workflow.await(deadline, result::isCompleted)) {
  scope.cancel("export deadline exceeded");   // Activity's next heartbeat throws
  throw ApplicationFailure.newFailure("export timed out", "ExportTimeout");
}
return result.get();
```

> Cancellation flows to the Activity via heartbeat; it cleans up partial work.

<!--
Example: examples/02-reliability/cancellation_scope.java
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Determinism, reinforced

The rules that keep replay honest.

<!--
Open in VSCode: examples/02-reliability/workflow_time.java - durable sleep records TimerStarted; no thread parks.
-->


---

# Common traps

* `Map.Entry.getKey()` iteration over `HashMap` - JVM-version-dependent.
* `Instant.now()`, `LocalDateTime.now()`.
* `UUID.randomUUID()` → use `Workflow.randomUUID()`.
* `CompletableFuture`, `ExecutorService` → use `Async.function`, `Workflow.newPromise`.
* Throwing checked exceptions across the Workflow boundary - prefer `ApplicationFailure`.

> The replay tests on Day 4 catch all of these.

<!--
Reinforcement, not new content.

The students saw the families yesterday; this is the "what bites in production"
list.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Signals and Queries

Push data in. Pull data out.

<!--
Open in VSCode: examples/03-interactions/signals_queries.java
Run: make run-approval
-->


---

<!-- _class: code -->

## Signals - push data in

```java
@WorkflowInterface
interface ApprovalWorkflow {
  @WorkflowMethod  String run(String requestId);
  @SignalMethod    void approve(String approver);
  @QueryMethod     String currentState();
}

@Override
public String run(String requestId) {
  Workflow.await(() -> state.startsWith("APPROVED"));
  return state;
}

@Override
public void approve(String approver) { state = "APPROVED by " + approver; }
```

<!--
Async, recorded in history, wakes any await predicate.
-->

---

# Queries - pull data out

```java
@Override
public String currentState() { return state; }
```

- Read-only function over **current in-memory state**.
- No history events. No Activities. No side effects.

> Synchronous and cheap. Routed to whichever Worker has the workflow cached.

---

<!-- _class: lab -->

###### Lab · Day 2

# Signals + Queries

Challenge → [`day-02-reliability/lab-2-signals-and-queries`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-02-reliability/lab-2-signals-and-queries.md)

```bash
make run-approval           # terminal 1: the Worker (stays up)
make run-approval-starter   # terminal 2: start the approval-demo Workflow

# then drive it from the CLI
temporal workflow signal --workflow-id approval-demo \
  --name approve --input '"alice"'
temporal workflow query --workflow-id approval-demo \
  --type currentState
```

> Send the Signal before the Workflow starts; see what happens.

<!--
The "before workflow starts" twist: signalWithStart later in the day will make
this explicit.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Updates

Synchronous, validated, write-capable RPC into a running Workflow.

<!--
Open in VSCode: examples/03-interactions/update_completed.java, update_with_start.java
Run: make run-approval
-->


---

<!-- _class: code -->

## @UpdateMethod + @UpdateValidatorMethod

<!-- Open in VSCode: examples/03-interactions/update_completed.java -->

```java
@WorkflowInterface
interface CartWorkflow {
  @WorkflowMethod  String checkout(String cartId);
  @UpdateMethod    int addItem(String sku, int quantity);

  @UpdateValidatorMethod(updateName = "addItem")
  void validateAddItem(String sku, int quantity);
}
```

- Caller blocks on `.getResult()`.
- Validator runs **before** the update is admitted to history.
- Reject cheaply; don't pollute the audit trail.

---

<!-- _class: code -->

## startUpdate - start now, get result later

```java
WorkflowStub stub = client.newUntypedWorkflowStub(workflowId);

WorkflowUpdateHandle<Integer> handle =
    stub.startUpdate("addItem", WorkflowUpdateStage.COMPLETED,
        Integer.class, "book", 2);

// ... do other work; the Update is already in flight ...
int itemCount = handle.getResult();   // block only when you need the value
```

- `WorkflowUpdateStage` is **required**: `ACCEPTED` or `COMPLETED`.
- A typed-stub `addItem(...)` call blocks outright; `startUpdate` hands back a handle.

<!--
Example: examples/03-interactions/update_completed.java
-->

---

<!-- _class: code -->

## signalWithStart

```java
BatchRequest batch = client.newSignalWithStartRequest();
batch.add(workflow::run, orderId);
batch.add(workflow::orderEvent, event);
client.signalWithStart(batch);
```

- First event for a key: workflow starts.
- Later events: signal the existing execution.

> Bare `start()` throws `WorkflowExecutionAlreadyStarted` on event #2.

<!--
This is THE foot-gun.

Every team copies bare WorkflowClient.start() from a tutorial and crashes on the
second Kafka message for the same key.
-->

---

<!-- _class: code -->

## startUpdateWithStart

<!-- Open in VSCode: examples/03-interactions/update_with_start.java -->

```java
WithStartWorkflowOperation<String> start =
    WithStartWorkflowOperation.newBuilder(workflow::process)
        .setArguments(request.orderId()).build();

WorkflowUpdateHandle<String> update =
    client.startUpdateWithStart(
        start, "submit", WorkflowUpdateStage.COMPLETED, String.class, request);

return update.getResult();
```

> One round trip. Creates the Workflow if absent, applies the Update, returns.

---

<!-- _class: lab -->

###### Lab · Day 2

# Updates

Challenge → [`day-02-reliability/lab-3-updates`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-02-reliability/lab-3-updates.md)

```bash
make run-approval           # Worker stays up
make run-approval-starter   # start the approval-demo Workflow
```

```bash
# Sync update against the running workflow
temporal workflow update execute --workflow-id approval-demo \
  --name changeNote --input '"expedite before close of business"'
```

> Verify the response is the **new workflow state**, not a generic 202.

<!--
The blocking-call shape is what makes Updates the modern primitive.

Signal+Query is older and works against older clusters; Update is the right tool
when the caller wants the result.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Schedules

Replacing Airflow's scheduler.

<!--
Open in VSCode: examples/03-interactions/schedule_interval.java, schedule_cron_overlap.java
Run: make run-schedules
-->


---

<!-- _class: code dense -->

## Hourly schedule

<!-- Open in VSCode: examples/03-interactions/schedule_interval.java -->

```java
Schedule schedule = Schedule.newBuilder()
    .setAction(ScheduleActionStartWorkflow.newBuilder()
        .setWorkflowType(OrdersWorkflow.class)
        .setOptions(WorkflowOptions.newBuilder().setTaskQueue("orders").build())
        .build())
    .setSpec(ScheduleSpec.newBuilder()
        .setIntervals(List.of(new ScheduleIntervalSpec(Duration.ofHours(1))))
        .setJitter(Duration.ofMinutes(5))
        .build())
    .setPolicy(SchedulePolicy.newBuilder()
        .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
        .build())
    .build();

scheduleClient.createSchedule("hourly-orders", schedule, ScheduleOptions.newBuilder().build());
```

> Durable Temporal object. Survives redeploy. Overlap is *explicit*.

---

<!-- _class: dense -->

## Cron, catchup & overlap

```java
ScheduleSpec.newBuilder()
    .setCronExpressions(List.of("0 9 * * *"))  // = Airflow schedule_interval
    .setJitter(Duration.ofMinutes(5)).build();
SchedulePolicy.newBuilder()
    .setCatchupWindow(Duration.ofHours(1))     // = Airflow catchup, but bounded
    .setOverlap(ScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP).build();
```

| Overlap policy | When a run is still going |
| --- | --- |
| `SKIP` | drop the new run |
| `BUFFER_ONE` / `BUFFER_ALL` | queue one / queue all |
| `ALLOW_ALL` | run concurrently |
| `CANCEL_OTHER` / `TERMINATE_OTHER` | stop the running one first |

<!--
Example: examples/03-interactions/schedule_cron_overlap.java
-->

---

<!-- _class: lab -->

###### Lab · Day 2

# Schedules

Challenge → [`day-02-reliability/lab-4-schedules`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-02-reliability/lab-4-schedules.md)

```bash
make run-schedules
temporal schedule list
temporal schedule describe --schedule-id daily-sales-report-schedule
```

Discuss:

- What does `SCHEDULE_OVERLAP_POLICY_SKIP` mean for a 90-minute job that fires hourly?
- Pause + resume from the CLI; observe what the schedule does.

<!--
Compare to "your DAG runs hourly but the 3 AM run takes 90 minutes" - in Airflow
you set max_active_runs.

Here you set ScheduleOverlapPolicy.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 2

# Child Workflows and timeouts

When to compose. How to bound.

<!--
Open in VSCode: examples/03-interactions/child_workflow.java, workflow_and_run_timeouts.java
-->


---

<!-- _class: code -->

## Child Workflows

<!-- Open in VSCode: examples/03-interactions/child_workflow.java -->

```java
FraudWorkflow fraud = Workflow.newChildWorkflowStub(FraudWorkflow.class,
    ChildWorkflowOptions.newBuilder().setTaskQueue("fraud").build());
ShippingWorkflow shipping = Workflow.newChildWorkflowStub(ShippingWorkflow.class,
    ChildWorkflowOptions.newBuilder().setTaskQueue("shipping").build());

Promise<String> fraudDecision = Async.function(fraud::check, orderId);
Promise<String> shippingPlan  = Async.function(shipping::plan, orderId);

Promise.allOf(fraudDecision, shippingPlan).get();
```

> Children get **independent identity, history, Task Queue, timeouts**.

---

# Workflow timeouts

```java
WorkflowOptions.newBuilder()
    .setWorkflowExecutionTimeout(Duration.ofDays(7))  // across continue-as-new
    .setWorkflowRunTimeout(Duration.ofHours(12))      // this run only
    .setTaskQueue("orders")
    .build();
```

- `WorkflowExecutionTimeout` - hard cap, all continuations.
- `WorkflowRunTimeout` - cap for this run; forces continuation.

<!--
Workflow timeouts ≠ Activity timeouts.

These are top-level execution caps, not per-attempt budgets.
-->

---

<!-- _class: takeaway -->

# Day 2 takeaways

* One async pattern: `Async.function` + `Promise.allOf`. Yields the Workflow loop, not threads.
* One Kafka/REST rule: **signalWithStart**, never bare start.
* One sync RPC: **startUpdateWithStart** for "POST and wait for result."

<!--
Three slogans for Day 2.

Each one is a foot-gun saved.
-->

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 3

# Kafka integration

Kafka is the bus between teams. Temporal is the brain inside one team.

<!--
Day 3 is half conceptual (where does Kafka end and Temporal start?) and half
hands-on (full Kafka → Temporal → Kafka loop).
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 3

# Temporal + Kafka architecture

Different jobs. Used together.

<!--
Open in VSCode: examples/04-kafka/kafka_consumer_activity.java, producer_activity_idempotent.java, outbox_activity.java
-->


---

<!-- _class: dense -->

# Who owns what

| Concern | Owner |
| --- | --- |
| Append-only event log, replayable by offset | Kafka |
| Fan-out to many independent consumers | Kafka |
| State of a single business transaction | Temporal |
| Retry / timeout / compensation logic | Temporal |
| Long-running human / external waits | Temporal |

> Kafka tells you *what happened*. Temporal tells you *where we are*.

---

<!-- _class: code -->

## Kafka consumer as Activity

```java
@Override
public List<String> pollBatch(String topic) {
  consumer.subscribe(List.of(topic));
  List<String> values = new ArrayList<>();
  while (values.size() < 100) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> record : records) {
      values.add(record.value());
      Activity.getExecutionContext()
          .heartbeat(record.topic() + ":" + record.partition() + ":" + record.offset());
    }
  }
  consumer.commitSync();   // commit ONLY after success
  return values;
}
```

<!--
Disable auto-commit.

Always.

Commit after the unit of work succeeds.

Heartbeat the topic:partition:offset so retries can resume.
-->

---

<!-- _class: code -->

## Producer Activity

<!-- Open in VSCode: examples/04-kafka/producer_activity_idempotent.java -->

```java
properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
properties.put(ProducerConfig.ACKS_CONFIG, "all");

@Override
public void publishOutcome(String orderId, String outcome) {
  producer.send(new ProducerRecord<>("order-outcomes", orderId, outcome)).join();
}
```

- Idempotent producer + stable key = at-least-once becomes effectively-once by key.
- Downstream still dedupes.

---

<!-- _class: code -->

## Outbox pattern

<!-- Open in VSCode: examples/04-kafka/outbox_activity.java -->

```java
transactionTemplate.execute(status -> {
  orderRepository.save(order);
  outboxRepository.save(new OutboxMessage(
      "order-events", order.id(),
      json.serialize(new OrderAccepted(order.id()))));
  return null;
});
```

- One DB transaction = atomic business row + outbox row.
- Publish to Kafka in a separate Activity / Debezium.

> Two side effects across systems can't be atomic without 2PC. Outbox sidesteps that.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 3

# Signal-driven Workflows

Replacing Kafka-triggered Airflow DAGs.

<!--
Open in VSCode: examples/04-kafka/signal_bridge.java - signalWithStart, not bare start.
Run: make run-kafka
-->


---

# The pattern

1) One Workflow per business entity (e.g. per orderId).
2) Workflow ID = `"order-" + orderId`.
3) Kafka consumer is a thin bridge: `signalWithStart` for every event.
4) Commit offsets after `signalWithStart` returns.

> Bare `start()` throws `WorkflowExecutionAlreadyStarted` on event #2.

---

<!-- _class: code -->

## The bridge

<!-- Open in VSCode: examples/04-kafka/signal_bridge.java -->

```java
BatchRequest batch = client.newSignalWithStartRequest();
batch.add(workflow::run, orderId);
batch.add(workflow::orderEvent, record.value());
client.signalWithStart(batch);
consumer.commitSync();
```

- First event for `orderId` starts the workflow.
- Subsequent events signal the existing execution.
- Offset commit happens *after* the signal lands.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 3

# End-to-end pipeline

Kafka → Temporal → Kafka.

<!--
Run: make run-kafka (Worker + bridge). Produce with make kafka-produce TOPIC=orders, tail with make kafka-consume TOPIC=order-outcomes.
-->


---

<!-- _class: lab -->

###### Lab · Day 3

# Run the pipeline

Challenge → [`day-03-kafka/lab-1-kafka-pipeline`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-03-kafka/lab-1-kafka-pipeline.md)

```bash
make stack-kafka      # KRaft broker on :9092
make run-kafka        # Worker + bridge

# in another terminal
kcat -b localhost:9092 -t orders -P -k "order-1" <<< 'NEW:line-item-A'
kcat -b localhost:9092 -t order-outcomes -C -o end -f 'key=%k value=%s\n'
```

> Send a second event for the same key. Watch it Signal the existing workflow.

<!--
Have students fire two events for one key.

The second event should NOT start a new workflow.

If it does, they're using bare start - debug it.
-->

---

# Partition fan-out

Two strategies:

* **Outside the Workflow** - one Workflow per partition. Many small histories.
* **Inside the Workflow** - one Workflow processes a *range* of partitions in parallel Activities.

> Pick based on whether the partitions share business state.

---

<!-- _class: code -->

## Inside-Workflow fan-out

<!-- Open in VSCode: examples/04-kafka/partition_fanout.java -->

```java
List<Promise<Integer>> counts =
    ranges.stream()
        .map(range -> Async.function(activities::processRange, range))
        .toList();

Promise.allOf(counts).get();
int total = counts.stream().mapToInt(Promise::get).sum();
```

Bound the fan-out: don't open 1,000 partitions inside one history.

---

<!-- _class: lab -->

###### Lab · Day 3

# Fan-out by partition

Challenge → [`day-03-kafka/lab-2-partition-fanout`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-03-kafka/lab-2-partition-fanout.md)

```bash
# Produce to multiple partitions (auto-create OR pre-create with 4)
make kafka-topic TOPIC=orders PARTITIONS=4

for i in 1 2 3 4; do
  echo "evt-$i" | kcat -b localhost:9092 -t orders -P -k "order-$i"
done
```

> In the Web UI, observe 4 separate Workflow executions, one per key.

---

# DLQ vs Temporal retry exhaustion

| Failure type | Belongs in |
| --- | --- |
| Transient (network) | Temporal retry (free) |
| Poison message (malformed) | DLQ topic for triage |
| Business rule rejection | DLQ or audit topic |
| Catch-all | DLQ after Temporal exhausts |

> Temporal retries solve transient. DLQ catches what retry can't fix.

---

<!-- _class: code -->

## DLQ Activity

<!-- Open in VSCode: examples/04-kafka/dlq_after_retry_exhaustion.java -->

```java
try {
  orders.validate(orderId);
} catch (ActivityFailure exhausted) {
  dlq.publish(orderId, exhausted.getMessage());
}
```

- Workflow catches `ActivityFailure` (retry exhausted).
- Publishes to DLQ; Workflow completes successfully.
- The *order* failed; the *Workflow* did its job.

---

<!-- _class: takeaway -->

# Day 3 takeaways

* `signalWithStart` is the only correct Kafka bridge primitive.
* Commit Kafka offsets only after the unit of work is durably accepted.
* DLQ catches what Temporal retries cannot fix. Different problems.

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 4

# Production engineering

Versioning, sizing, observability, replay tests, namespaces, Airflow migration.

<!--
Heaviest day on production rigour.

Lots of ops content.

Two big labs: metrics dashboard and replay tests.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Workflow versioning

Shipping new code without breaking in-flight Workflows.

<!--
Open in VSCode: examples/05-production/get_version_patch.java, versioning_behavior.java
-->


---

# Why versioning exists

Day 1: deploy v1. Workflow runs against v1 history.

Day 30: deploy v2 that reorders two Activities.

In-flight Workflow resumes against **v2 code** with **v1 history** → non-determinism error.

> You need v2 code to behave like v1 *until past the change-point*.

---

<!-- _class: code -->

## `Workflow.getVersion`

<!-- Open in VSCode: examples/05-production/get_version_patch.java -->

```java
int v = Workflow.getVersion("charge-before-reserve", Workflow.DEFAULT_VERSION, 1);

if (v == Workflow.DEFAULT_VERSION) {
  payments.reserve(orderId);
  payments.charge(orderId);
} else {
  payments.charge(orderId);
  payments.reserve(orderId);
}
```

> The change-point name is *identity*. Treat like a migration filename. Never recycle.

---

<!-- _class: code -->

## Versioning behavior

<!-- Open in VSCode: examples/05-production/versioning_behavior.java -->

```java
@WorkflowVersioningBehavior(VersioningBehavior.PINNED)
class ShortLivedCheckoutWorkflow implements CheckoutWorkflow { ... }

@WorkflowVersioningBehavior(VersioningBehavior.AUTO_UPGRADE)
class SubscriptionLifecycleWorkflow implements SubscriptionWorkflow { ... }
```

- **PINNED** - drain in-flight on old Workers, then deploy.
- **AUTO_UPGRADE** - long-runners pick up newer compatible code automatically.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Worker sizing & Task Queue design

Sized for resource profile, not business domain.

<!--
Open in VSCode: examples/05-production/worker_options_manual.java, worker_tuner.java, composite_tuner.java, virtual_threads.java
-->


---

<!-- _class: dense -->

# The levers

| Setting | Controls |
| --- | --- |
| `maxConcurrentWorkflowTaskExecutionSize` | In-flight workflow decisions on this Worker |
| `maxConcurrentActivityExecutionSize` | In-flight Activity attempts |
| `ResourceBasedTuner` | Auto-scale Worker slots vs CPU / memory targets |
| `CompositeTuner` | Mix strategies: fixed workflow slots + resource-based activity slots |
| Sticky execution | Worker caches workflows; skips full replay each task |
| `setUsingVirtualThreads(true)` (JDK 21+) | Threads = cheaper; more Activity concurrency |
| Number of Task Queues | One pool per resource profile |

---

<!-- _class: code -->

## Manual sizing

<!-- Open in VSCode: examples/05-production/worker_options_manual.java -->

```java
Worker worker = factory.newWorker(
    "io-heavy",
    WorkerOptions.newBuilder()
        .setMaxConcurrentActivityExecutionSize(200)
        .setMaxConcurrentWorkflowTaskExecutionSize(20)
        .build());
```

> I/O-heavy workload: many concurrent Activities, few workflow tasks.

---

<!-- _class: code -->

## Resource-based tuner

<!-- Open in VSCode: examples/05-production/worker_tuner.java -->

```java
ResourceBasedTuner tuner =
    ResourceBasedTuner.newBuilder()
        .setControllerOptions(
            ResourceBasedControllerOptions.newBuilder()
                .setTargetMemoryUsage(0.75)
                .setTargetCpuUsage(0.80)
                .build())
        .build();

Worker worker = factory.newWorker(
    "payments", WorkerOptions.newBuilder().setWorkerTuner(tuner).build());
```

> Auto-fit Worker slot counts to host capacity. Best fit for mixed workloads.

---

<!-- _class: code -->

## CompositeTuner - mix strategies

```java
ResourceBasedController controller =
    ResourceBasedController.newSystemInfoController(
        ResourceBasedControllerOptions.newBuilder()
            .setTargetMemoryUsage(0.75).setTargetCpuUsage(0.80).build());

WorkerTuner tuner = new CompositeTuner(
    new FixedSizeSlotSupplier<>(20),                       // workflow task slots
    ResourceBasedSlotSupplier.createForActivity(           // activity slots
        controller, ResourceBasedSlotOptions.getDefaultInstance()),
    new FixedSizeSlotSupplier<>(20));                      // local activity slots
```

> Fixed where load is predictable; resource-based where it isn't.

<!--
Example: examples/05-production/composite_tuner.java
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Observability

Metrics on day one.

<!--
Open in VSCode: examples/05-production/micrometer_metrics.java, custom_activity_metric.java, otel_tracing.java
Stack: make stack-obs, then make grafana
-->


---

<!-- _class: dense -->

# Key SDK metrics

| Metric | Tells you |
| --- | --- |
| `temporal_workflow_task_schedule_to_start_latency` | Worker capacity vs demand |
| `temporal_workflow_completed_total` | Throughput |
| `temporal_workflow_failed_total` | Real failures |
| `temporal_activity_execution_failed_total` | Bad downstream / retry config |
| `temporal_sticky_cache_size` | Replay overhead / memory health |
| `temporal_activity_schedule_to_start_latency` | Activity backlog |

> Wire via Micrometer → Prometheus → your existing Grafana.

---

<!-- _class: code -->

## Micrometer wiring

<!-- Open in VSCode: examples/05-production/micrometer_metrics.java -->

```java
PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

Scope scope = new RootScopeBuilder()
    .reporter(new MicrometerClientStatsReporter(registry))
    .reportEvery(com.uber.m3.util.Duration.ofSeconds(10));

WorkflowServiceStubs service = WorkflowServiceStubs.newServiceStubs(
    WorkflowServiceStubsOptions.newBuilder().setMetricsScope(scope).build());
```

---

<!-- _class: code -->

## Custom Activity metric

```java
class InvoiceActivitiesImpl implements InvoiceActivities {
  private final Counter invoices;
  InvoiceActivitiesImpl(MeterRegistry registry) {
    this.invoices = Counter.builder("training_invoices_generated_total").register(registry);
  }
  @Override public String generateInvoice(String orderId) {
    invoices.increment();
    return "s3://invoices/" + orderId + ".pdf";
  }
}
```

> Same Micrometer registry as the SDK metrics; your KPIs sit beside Temporal's.

<!--
Example: examples/05-production/custom_activity_metric.java
-->

---

<!-- _class: code -->

## Tracing with OpenTelemetry

```java
client = WorkflowClient.newInstance(service,
    WorkflowClientOptions.newBuilder()
        .setInterceptors(new OpenTracingClientInterceptor(otOptions))
        .build());

factory = WorkerFactory.newInstance(client,
    WorkerFactoryOptions.newBuilder()
        .setWorkerInterceptors(new OpenTracingWorkerInterceptor())
        .build());
```

> One trace spans client → Workflow → Activity. Needs `temporal-opentracing`.

<!--
Example: examples/05-production/otel_tracing.java
-->

---

<!-- _class: lab -->

###### Lab · Day 4

# Local dashboard

Challenge → [`day-04-production/lab-1-observability`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-04-production/lab-1-observability.md)

```bash
make stack-obs        # Prometheus + Grafana
make temporal         # dev server with --metrics-port 7234
open http://localhost:3000
make load-transform N=50
```

In Grafana, open the **Temporal Training - Overview** dashboard and watch:

1. `temporal_workflow_completed_total` climb.
2. `temporal_workflow_failed_total` increment when you force a failure.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Namespace strategy

Isolation boundary, not a routing primitive.

<!--
Open in VSCode: examples/05-production/namespace_strategy.md
-->


---

<!-- _class: dense -->

# When to split namespaces

| Scenario | Namespace shape |
| --- | --- |
| Dev / staging / prod | One namespace per environment |
| Regulated tenant isolation | One namespace per tenant |
| Shared SaaS tenants | One namespace per env; tenant ID in Search Attributes |
| Different retention SLAs | Separate namespace per retention class |

> Namespace ≠ Task Queue. Task Queue routes work; Namespace bounds it.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Testing

In-process Workflow tests with time skipping.

<!--
Open in VSCode: examples/05-production/junit5_extension_mockito_test.java; runnable test in examples/runnable/06-testing/ReminderWorkflowTest.java
Run: make run-testing (no server needed)
-->


---

<!-- _class: code -->

## TestWorkflowEnvironment

<!-- Open in VSCode: examples/runnable/06-testing/src/test/java/training/temporal/testing/ReminderWorkflowTest.java · Run: make run-testing -->

```java
TestWorkflowEnvironment env = TestWorkflowEnvironment.newInstance();
Worker worker = env.newWorker("reminder");
worker.registerWorkflowImplementationTypes(ReminderWorkflowImpl.class);
worker.registerActivitiesImplementations(new ReminderActivitiesImpl());
env.start();

ReminderWorkflow stub = env.getWorkflowClient().newWorkflowStub(
    ReminderWorkflow.class,
    WorkflowOptions.newBuilder().setTaskQueue("reminder").build());

String result = stub.run("hello");
```

> No Docker. No network. *Time skipping* - a 30-day reminder completes in milliseconds.

---

<!-- _class: code -->

## JUnit 5 extension + mocked Activities

```java
@RegisterExtension
static final TestWorkflowExtension ext = TestWorkflowExtension.newBuilder()
    .setWorkflowTypes(ReminderWorkflowImpl.class).setDoNotStart(true).build();
@Test
void completes(TestWorkflowEnvironment env, Worker worker, ReminderWorkflow wf) {
  ReminderActivities activities = mock(ReminderActivities.class);
  when(activities.lookupEmail("u1")).thenReturn("u1@example.com");
  worker.registerActivitiesImplementations(activities);
  env.start();
  assertEquals("sent to u1@example.com", wf.remind("u1"));
}
```

> Extension injects env/worker/stub; Mockito mocks Activities - zero I/O.

<!--
Example: examples/05-production/junit5_extension_mockito_test.java
-->

---

<!-- _class: lab -->

###### Lab · Day 4

# In-process tests

Challenge → [`day-04-production/lab-2-testing-workflows`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-04-production/lab-2-testing-workflows.md)

```bash
make run-testing
```

- The test uses `Workflow.sleep(Duration.ofDays(1))`.
- It still completes in <1 second.
- Try changing the sleep to 30 days; same test time.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Workflow replay testing

Catching determinism regressions before they reach production.

<!--
Open in VSCode: examples/05-production/replay_test.java
-->


---

<!-- _class: code -->

## Capture & replay

<!-- Open in VSCode: examples/05-production/replay_test.java -->

```bash
# Capture
temporal workflow show --workflow-id order-1001 \
  --output json > histories/order-1001.json
```

```java
// Replay
@Test
void replaysProductionHistory() throws Exception {
  WorkflowReplayer.replayWorkflowExecutionFromResource(
      "histories/order-1001.json", OrderSagaWorkflowImpl.class);
}
```

> Refactor breaks an in-flight workflow → CI fails before you ship.

---

<!-- _class: lab -->

###### Lab · Day 4

# Build a replay corpus

Challenge → [`day-04-production/lab-3-replay-testing`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-04-production/lab-3-replay-testing.md)

For a Workflow you wrote on Day 1-2:

1. Run 3 executions covering: happy path, retry, cancellation.
2. Capture each with `temporal workflow show ... --output json`.
3. Drop them into `src/test/resources/histories/`.
4. Add a `WorkflowReplayer` test per file.
5. Modify the Workflow to reorder Activities; watch the test fail.

<!--
This is the safety net for the rest of the year.

Encourage students to take this pattern back to their team and seed a corpus.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 4

# Migrating Airflow DAGs

A decision framework.

---

<!-- _class: dense -->

# Migrate or not?

| DAG shape | Verdict |
| --- | --- |
| Simple ETL on a fixed schedule | Stay on Airflow, or move scheduler to Temporal Schedules |
| Cross-system orchestration with retries and human steps | Migrate (sweet spot) |
| Kafka-triggered, one execution per key | Migrate (Day 3 pattern) |
| Pure data transformation | Don't migrate. Spark / dbt territory |
| Long-running waits (hours, days, humans) | Migrate. Airflow handles this poorly |
| Tight Airflow operator coupling | Wrap in Activities; the operator is the unit |

---

# Migration order that works

1) **Pick one** DAG that hurts in production.
2) **Map operators → Activities** mechanically. Don't redesign.
3) **Run side by side** for a release cycle.
4) **Cut over** after the Temporal version is clean for two weeks.
5) **Redesign** only after stable. Now use Signals, Updates, Schedules.

> Don't migrate everything. Migrate where Temporal earns its keep.

---

<!-- _class: takeaway -->

# Day 4 takeaways

* Versioning is about preserving old histories, not just deploying new code.
* Size Workers for **resource profile**, not business domain.
* Replay tests are the single safety net for Workflow code changes.
* Not everything is a Workflow. Migrate where Temporal earns its keep.

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 5

# Saga, Spring Boot & capstone

Real-world Workflow walkthrough. Then build one.

<!--
4 hours.

Morning is saga + Spring.

Afternoon is capstone (75 min of build time + 25 min review + 20 min Q&A).
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 5

# Order-processing saga

The canonical demo: payment → inventory → ship; compensate on failure.

<!--
Open in VSCode: examples/06-saga-spring/saga_compensation.java (full project: examples/runnable/07-saga/)
Run: make run-saga
-->


---

<!-- _class: code dense -->

## The saga

<!-- Open in VSCode: examples/06-saga-spring/saga_compensation.java (full project: examples/runnable/07-saga/) -->

```java
public String process(String orderId) {
  Saga saga = new Saga(new Saga.Options.Builder().setParallelCompensation(false).build());
  try {
    String paymentId = activities.authorizePayment(orderId);
    saga.addCompensation(activities::cancelPayment, paymentId);

    String reservationId = activities.reserveInventory(orderId);
    saga.addCompensation(activities::restoreInventory, reservationId);

    activities.ship(orderId);
    return "COMPLETED";
  } catch (RuntimeException failure) {
    saga.compensate();   // LIFO
    activities.sendFailureNotification(orderId, failure.getMessage());
    return "COMPENSATED";
  }
}
```

---

# Orchestration vs choreography

* **Orchestration** - one central Workflow coordinates all steps & compensations. Single audit trail. **Temporal's natural shape.**
* **Choreography** - each service reacts to events, emits its own. No central state.

> For cross-team flows from Airflow + Kafka, orchestration wins.

<!--
War story: when team #3 silently drops an event in a choreographed flow, nobody
notices for 36 hours.

Temporal's log shows it immediately.
-->

---

# Compensation rules

1) Register compensation **immediately** after the forward step succeeds.
2) Compensations are **business logic**, not generic undo.
3) Compensations get their own retry policy. Test the failing case.
4) Idempotency on forward AND compensation steps.

---

<!-- _class: lab -->

###### Lab · Day 5

# Run the saga

Challenge → [`day-05-saga-spring/lab-1-order-saga-walkthrough`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-05-saga-spring/lab-1-order-saga-walkthrough.md)

```bash
make run-saga
```

Start the happy path:

```bash
temporal workflow start --task-queue orders \
  --type OrderSagaWorkflow --workflow-id order-OK \
  --input '"order-1001"'
```

---

# Run the saga: force a failure

```bash
temporal workflow start --task-queue orders \
  --type OrderSagaWorkflow --workflow-id order-fail \
  --input '"fail-at-ship"'
```

> In the Web UI, watch the compensations fire in reverse order.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 5

# Saga in Spring Boot

Wiring + interaction patterns.

<!--
Open in VSCode: examples/06-saga-spring/spring_temporal_config.java, kafka_listener_trigger.java, sync_saga_update.java, async_saga_signal.java, continue_as_new.java
-->


---

<!-- _class: code -->

## Manual Spring config

```java
@Configuration
class TemporalConfig {
  @Bean WorkflowServiceStubs workflowServiceStubs() {
    return WorkflowServiceStubs.newLocalServiceStubs(); }
  @Bean WorkflowClient workflowClient(WorkflowServiceStubs s) {
    return WorkflowClient.newInstance(s); }
  @Bean(initMethod = "start", destroyMethod = "shutdown")
  WorkerFactory workerFactory(WorkflowClient c, OrderActivities a) {
    WorkerFactory f = WorkerFactory.newInstance(c);
    Worker w = f.newWorker("orders");
    w.registerWorkflowImplementationTypes(OrderSagaWorkflowImpl.class);
    w.registerActivitiesImplementations(a);
    return f;
  }
}
```

<!--
This is the underlying wiring.

In production, prefer the temporal-spring-boot-starter and let it do this.
-->

---

<!-- _class: code -->

## Sync interaction (Update)

<!-- Open in VSCode: examples/06-saga-spring/sync_saga_update.java -->

```java
WithStartWorkflowOperation<String> start =
    WithStartWorkflowOperation.newBuilder(workflow::process)
        .setArguments(request.orderId()).build();

WorkflowUpdateHandle<String> update = client.startUpdateWithStart(
    start, "submit", WorkflowUpdateStage.COMPLETED, String.class, request);

return update.getResult();
```

> POST endpoint blocks until the workflow returns. One round trip.

---

<!-- _class: code -->

## Async interaction (Signal)

<!-- Open in VSCode: examples/06-saga-spring/async_saga_signal.java -->

```java
@KafkaListener(topics = "orders")
void onOrder(OrderRequest request) {
  OrderSagaWorkflow workflow = client.newWorkflowStub(
      OrderSagaWorkflow.class,
      WorkflowOptions.newBuilder()
          .setWorkflowId("order-" + request.orderId())
          .setTaskQueue("orders").build());

  BatchRequest batch = client.newSignalWithStartRequest();
  batch.add(workflow::process, request.orderId());
  batch.add(workflow::onUpdate, request);
  client.signalWithStart(batch);
}
```

---

<!-- _class: code -->

## Continue-as-new

<!-- Open in VSCode: examples/06-saga-spring/continue_as_new.java -->

```java
@Override
public void run(String subscriptionId, int eventCount) {
  while (true) {
    Workflow.await(this::hasNextEvent);
    handleNextEvent();
    eventCount++;
    if (eventCount >= 1000) {
      Workflow.continueAsNew(subscriptionId, 0);
    }
  }
}
```

> Continue-as-new is a *checkpoint*, not a memory dump. Carry only what's needed.

---

<!-- _class: lab -->

###### Lab · Day 5

# Saga in Spring Boot

Challenge → [`day-05-saga-spring/lab-2-saga-spring-boot`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-05-saga-spring/lab-2-saga-spring-boot.md)

Wire the saga into a Spring Boot app:

1. Register the Worker as a `@Component` with `WorkerFactory` lifecycle bound to the app context.
2. Drive the Workflow from a `@RestController` — start, signal, query.
3. Inject Activity dependencies (DB, HTTP clients) as Spring beans.

> Goal: the saga runs inside Spring Boot, started from an HTTP endpoint.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 5

# Capstone

Redesign a Kafka-triggered Airflow DAG as a Temporal Saga.

<!--
Scaffold from examples/runnable/07-saga/ (Run: make run-saga). Challenge: day-05-saga-spring/lab-3-capstone.
-->


---

# The task

> A customer signup flow. Kafka event `customer-signup` arrives with `{userId, email, plan}`. The DAG runs four tasks: create user, charge first month, provision tenant, send welcome email. Failure handling today is ad hoc.

Redesign it as a Saga. Demonstrate one compensation path.

---

# Deliverable plan (75 min)

| Time | Deliverable |
| --- | --- |
| 0-10 | Sketch Workflow signature + Activity interface + compensation order on paper |
| 10-50 | Implement enough Java to run the happy path + one failure path |
| 50-65 | Wire the Kafka trigger with `signalWithStart` |
| 65-75 | Run end-to-end against the local stack; demo one compensation |

---

# Acceptance criteria

1. At least three forward steps.
2. Compensation registered immediately after each step.
3. `@KafkaListener` triggering via `signalWithStart`.
4. One demonstrated failure → compensation visible in the Web UI history.

---

<!-- _class: lab -->

###### Lab · Day 5

# Capstone

Challenge → [`day-05-saga-spring/lab-3-capstone`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-05-saga-spring/lab-3-capstone.md)

```bash
make stack-kafka
make temporal
# Use 07-saga or scaffold your own
```

Go. 75 minutes. Walk the room every 15. Unstick people on Spring config -
the lesson is in the saga shape, not the wiring.

<!--
Hold the line on time.

At 50 minutes, stop everyone and check in.

If most are stuck, slow down; if most are done, pull review forward.
-->

---

# Capstone review (25 min)

Two or three volunteer pairs share screen. The room critiques. Cover:

- How did they decide what was a Workflow vs an Activity?
- Where did they put idempotency keys?
- Orchestration or choreography? Why?
- What would they change for a 30-day saga?

<!--
Resist correcting code style.

Focus on the four questions above.

They are what the cohort will face on real systems.
-->

---

# Q&A + open migration planning (20 min)

Anchor questions if the room is quiet:

- Pick one Airflow DAG. What's the first thing that would break in Temporal?
- What's your team's hardest distributed-transaction failure? Would a Saga have caught it?
- Where does "I think it ran but I'm not sure" happen in your stack? That's a Workflow.

---

<!-- _class: takeaway -->

# Day 5 takeaways

* Compensation is business logic, not generic undo. Design it on purpose.
* Sync Updates via `startUpdateWithStart` for sync APIs.
* Async Signals via `signalWithStart` for event-driven triggers.
* Continue-as-new is a checkpoint, not a memory dump.

---

<!-- _class: day -->
<!-- _transition: zoom 0.6s -->

###### Day 6

# AWS migration & container workloads

Replacing Glue + Lambda + Step Functions. Running Workers in Kubernetes.

<!--
4 hours: morning is AWS migration; afternoon is containers + KEDA.

Two big labs: containerized Worker on kind, and KEDA autoscale.
-->

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 6

# The AWS orchestration problem

Hidden complexity in Lambda + Glue + Step Functions.

<!--
Open in VSCode: examples/07-aws-containers/aws_mapping.md, step_functions_before.json vs step_functions_after_temporal.java
-->


---

# State scatters

* **EventBridge** rule fires.
* **Lambda** validates, transforms, sometimes orchestrates.
* **Step Functions** declares state transitions in JSON.
* **Glue** runs Spark / Python shell, writes results to S3.
* **S3** is the handoff medium.

> Every handoff = a chance for state to disagree. Recovery is a runbook.

---

<!-- _class: dense -->

# AWS → Temporal map

| AWS shape | Temporal shape |
| --- | --- |
| EventBridge → Lambda → Step Functions | Consumer (or thin Lambda) `signalWithStart`s a Workflow |
| Step Functions JSON states | Workflow branches through Java code |
| Glue Python writes S3 checkpoints | Activity returns result; history records the run |
| Glue Spark heavy transform | Activity starts Glue, heartbeats runId while polling |
| CloudWatch Lambda retry | `RetryOptions` with typed `ApplicationFailure` |
| S3 handoff between Lambdas | Activity returns S3 URI |
| DynamoDB checkpoint table | Workflow event history |

---

<!-- _class: dense -->

# When to keep AWS compute

| Service | Keep when | Replace when |
| --- | --- | --- |
| Lambda | <100ms, IAM-bound, one-shot | Multi-step coordination, retries, long waits |
| Glue Spark | Large distributed transforms (>10 GB) | Pure data movement; small batches |
| Glue Python | Tiny scripts (<1 min) with Glue catalog | Anything you'd write as a Java Activity |
| Step Functions | Already wired, low-change | Anything needing human steps or code review |

> Temporal supervises; AWS executes the heavy lift.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 6

# Glue Spark as an Activity

The canonical supervise-AWS-compute pattern.

<!--
Open in VSCode: examples/07-aws-containers/glue_activity.java, s3_reference_payload.java
Run: make run-aws
-->


---

<!-- _class: code dense -->

## Glue activity

<!-- Open in VSCode: examples/07-aws-containers/glue_activity.java · Run: make run-aws -->

```java
@Override
public String runGlueJob(String jobName, String inputS3Uri) {
  String runId = glue.startJobRun(
      StartJobRunRequest.builder()
          .jobName(jobName)
          .arguments(Map.of("--input", inputS3Uri))
          .build()).jobRunId();

  while (true) {
    Activity.getExecutionContext().heartbeat(runId);
    JobRun jobRun = glue.getJobRun(
        GetJobRunRequest.builder().jobName(jobName).runId(runId).build()).jobRun();
    if (jobRun.jobRunState() == JobRunState.SUCCEEDED) return runId;
    if (Set.of(FAILED, TIMEOUT, STOPPED).contains(jobRun.jobRunState()))
      throw ApplicationFailure.newFailure(jobRun.errorMessage(), "GlueJobFailed");
    Thread.sleep(Duration.ofSeconds(15).toMillis());   // back off
  }
}
```

---

<!-- _class: lab -->

###### Lab · Day 6

# Glue Activity (LocalStack)

Challenge → [`day-06-aws-containers/lab-1-glue-activity`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-06-aws-containers/lab-1-glue-activity.md)

```bash
make stack-aws        # LocalStack on :4566
make aws-init         # create S3 buckets
make run-aws          # Import Worker

# in another terminal
awslocal s3 cp /tmp/test.csv s3://imports-incoming/test.csv
scripts/start-workflow.sh transform 1 ImportWorkflow "s3://imports-incoming/test.csv"
```

> Watch the heartbeats in the Web UI as the polling loop runs.

---

# S3 reference payloads

```java
record TransformRequest(String inputS3Uri, String outputPrefix) {}
record TransformResult(String outputS3Uri, long rowCount) {}
```

- Workflow history holds **URIs + counts**.
- Activity owns the bytes.
- Hard cap **2 MB** per payload (SDK warns ~256 KB); large data via S3.

> Workflow history is small. URIs travel cheap.

---

<!-- _class: code -->

## Codec server - encrypt payloads

```java
class EncryptionCodec implements PayloadCodec {
  public List<Payload> encode(List<Payload> p) { /* AES-GCM encrypt */ }
  public List<Payload> decode(List<Payload> p) { /* decrypt        */ }
}

DataConverter converter = new CodecDataConverter(
    DefaultDataConverter.newDefaultInstance(), List.of(new EncryptionCodec()));

WorkflowClient.newInstance(service,
    WorkflowClientOptions.newBuilder().setDataConverter(converter).build());
```

> Server stores ciphertext only. A standalone codec server lets the Web UI decode on demand.

<!--
Example: examples/07-aws-containers/codec_server.java
-->

---

<!-- _class: lab -->

###### Lab · Day 6

# Replace S3 checkpoints

Challenge → [`day-06-aws-containers/lab-2-s3-checkpointing`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-06-aws-containers/lab-2-s3-checkpointing.md)

Take a hypothetical existing pipeline that writes a checkpoint S3 key after every step.

1. Identify which checkpoints are *resume points*. Those become Workflow state.
2. Identify which checkpoints are *handoffs*. Those become Activity return URIs.
3. Sketch the Workflow signature. What's input? What's output?

> No new code; redesign on paper. 15 minutes.

---

<!-- _class: code -->

## Step Functions → Temporal

<!-- Open in VSCode: examples/07-aws-containers/step_functions_before.json vs step_functions_after_temporal.java -->

```java
@Override
public void run(String inputS3Uri) {
  ValidatedFile file = activities.validate(inputS3Uri);
  String transformedUri = activities.transform(file.cleanInputUri());
  LoadResult loadResult = activities.load(transformedUri);
  activities.publishNotification(loadResult);
}
```

Compare to the equivalent ASL: ~30 lines of JSON state machine with `Resource` arns.

---

<!-- _class: lab -->

###### Lab · Day 6

# Migrate a Step Functions pipeline

Challenge → [`day-06-aws-containers/lab-3-stepfunctions-migration`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-06-aws-containers/lab-3-stepfunctions-migration.md)

Take a four-state ASL state machine (validate → transform → load → notify):

1. Map each `Task` state to an Activity; the state machine becomes Workflow code.
2. Replace `Retry` / `Catch` blocks with Temporal `RetryOptions` + try/catch.
3. Run it end-to-end against LocalStack.

> Goal: the ASL JSON is gone; control flow lives in Workflow code.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Day 6

# Workers as containers

No HTTP server. Process-level probes. Graceful shutdown.

<!--
Open in VSCode: examples/07-aws-containers/Dockerfile, worker_deployment.yaml, keda_scaledobject.yaml
-->


---

# Mental model

* A Worker is a long-lived process polling Task Queues *outbound*.
* **No inbound traffic.** No Service, no Ingress.
* Health = "is the process polling?" `pgrep` exec probe, or an HTTP `/health` (Actuator/`HttpServer`) returning 200 only after `WorkerFactory.start()`.
* Graceful shutdown = drain in-flight Activities; SIGTERM, then heartbeat-cancel.

---

<!-- _class: code -->

## Dockerfile

<!-- Open in VSCode: examples/07-aws-containers/Dockerfile (runnable: examples/runnable/08-aws-containers/Dockerfile) -->

```dockerfile
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/worker.jar /app/worker.jar
ENV JAVA_TOOL_OPTIONS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75"
ENTRYPOINT ["java", "-jar", "/app/worker.jar"]
```

---

<!-- _class: lab -->

###### Lab · Day 6

# Docker build

Challenge → [`day-06-aws-containers/lab-4-worker-container`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-06-aws-containers/lab-4-worker-container.md)

```bash
cd examples/runnable/08-aws-containers
docker build -t temporal-transform-worker:dev .

docker run --rm \
  -e TEMPORAL_ADDRESS=host.docker.internal:7233 \
  temporal-transform-worker:dev
```

> Confirm the Worker connects to the host's Temporal and starts polling.

---

<!-- _class: code dense -->

## Kubernetes Deployment

<!-- Open in VSCode: examples/07-aws-containers/worker_deployment.yaml -->

```yaml
spec:
  replicas: 2
  strategy:
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  template:
    spec:
      terminationGracePeriodSeconds: 120   # >= longest Activity timeout
      containers:
        - name: worker
          image: <ecr>/temporal-transform-worker:latest
          readinessProbe:
            exec: { command: ["sh", "-c", "pgrep -f worker.jar > /dev/null"] }
          livenessProbe:
            exec: { command: ["sh", "-c", "pgrep -f worker.jar > /dev/null"] }
```

> `terminationGracePeriodSeconds` ≥ longest `startToCloseTimeout`.

---

<!-- _class: lab -->

###### Lab · Day 6

# K8s deploy on kind

```bash
make kind-up          # cluster + KEDA
make kind-load        # build + load Worker image
kubectl apply -f examples/runnable/08-aws-containers/k8s-worker-deployment.yaml
kubectl rollout status deployment/temporal-transform-worker
kubectl logs -l app=temporal-transform-worker --tail=20
```

> Confirm the Worker polls the cluster's Temporal address.

---

<!-- _class: code -->

## KEDA temporal scaler

<!-- Open in VSCode: examples/07-aws-containers/keda_scaledobject.yaml -->

```yaml
kind: ScaledObject
spec:
  scaleTargetRef: { name: temporal-transform-worker }
  minReplicaCount: 1
  maxReplicaCount: 10
  triggers:
    - type: temporal
      metadata:
        endpoint: temporal-frontend.temporal.svc.cluster.local:7233
        namespace: production
        taskQueue: transform
        queueType: ActivityTaskQueue
        targetQueueSize: "20"
```

> Native scaler polls `DescribeTaskQueue`. No Prometheus exporter needed.

---

<!-- _class: lab -->

###### Lab · Day 6

# KEDA autoscale

Challenge → [`day-06-aws-containers/lab-5-kubernetes-keda`](https://github.com/codermana/Temporal-Training/blob/master/challenges/day-06-aws-containers/lab-5-kubernetes-keda.md)

```bash
kubectl apply -f examples/runnable/08-aws-containers/keda-scaledobject.yaml
make load-transform N=200
kubectl get scaledobject,pods -l app=temporal-transform-worker -w
```

> Watch replica count climb from 1 → ~5 as backlog grows.

---

# Glue Activities in containers

* Worker pod runs Glue-orchestration Activities.
* **IRSA**, not access keys: `eks.amazonaws.com/role-arn` on the ServiceAccount.
* Outbound to Temporal frontend (Cloud or self-hosted ELB).
* Outbound to AWS APIs via VPC endpoints.

> No bundled access keys. IRSA + VPC endpoints is the production shape.

---

<!-- _class: dense -->

# Temporal Cloud vs EKS self-hosted

| Concern | Temporal Cloud | EKS self-hosted |
| --- | --- | --- |
| Setup time | Hours | Weeks |
| Persistence | Managed (Cassandra) | You run PostgreSQL / Cassandra |
| Upgrades | Automatic | You schedule |
| Multi-region | Built-in (premium) | You design replication |
| Cost shape | Per-action | Fixed infra |
| Audit / compliance | SOC2, HIPAA tiers | You provide evidence |

> Use Cloud unless you have a specific reason not to.

---

<!-- _class: lab -->

###### Lab · Day 6

# End-to-end S3 → Temporal → S3

```bash
make stack-aws        # LocalStack
make temporal         # dev server
make run-aws          # Worker
awslocal s3 cp test-input.csv s3://imports-incoming/
scripts/start-workflow.sh transform end2end ImportWorkflow "s3://imports-incoming/test-input.csv"
awslocal s3 ls s3://imports-output/
```

Verify:

- Three Activity completions in the Web UI.
- Workflow history holds URIs + `rowCount`, not file bytes.

---

<!-- _class: takeaway -->

# Day 6 takeaways

* Temporal replaces orchestration **state**, not all compute. Keep Glue Spark; replace Step Functions JSON.
* Workers have no inbound traffic. Use `exec` probes or add Actuator deliberately.
* KEDA's native Temporal scaler is the right one.
* Cloud is the default for new deployments. Self-host only with a clear reason.

---

<!-- _class: section -->
<!-- _transition: slide 0.5s -->

###### Course close

# What you have now

A complete Temporal mental model and the patterns to ship with.

---

# Where to go next

* Take the **capstone** from Day 5 back to your team. Ship it side-by-side with the existing implementation.
* Stand up the **observability stack** in your real env. Get the metrics flowing first.
* Start the **replay corpus**. One captured history per non-trivial Workflow.
* Pick one **Airflow DAG** to migrate using the framework.

---

<!-- _class: takeaway -->

# The four habits

1) When you'd write a runbook, write a Workflow instead.
2) Workflow code is deterministic; all I/O lives in Activities.
3) `signalWithStart` / `startUpdateWithStart` are the bridge primitives.
4) Capture histories; replay them in CI.

---

<!-- _class: quote -->

> Your hardest distributed-transaction bug today is a feature Temporal already solved.

The cost is learning a new model. The reward is fewer runbooks.

---

## Resources

- **Docs** — https://docs.temporal.io
- **Java SDK** — https://github.com/temporalio/sdk-java
- **Slides** — https://temporal-training.slides.algogrit.com/temporal-fundamentals/
- **Course repo** — https://github.com/CoderMana/temporal-training
- **Hands-on labs** — https://github.com/codermana/Temporal-Training/tree/master/challenges
