# Day 2 — Building reliable workflows

Today you make Workflows do real work: run Activities in parallel, interact with
running executions from the outside (signals, queries, updates), and replace
Airflow's scheduler with Temporal Schedules.

## Required stack

Temporal dev server only:

```bash
make temporal
```

## Labs

| # | Lab | Time | Difficulty |
|---|-----|------|-----------|
| 1 | [Async & parallel Activities](lab-1-async-parallel-activities.md) | 50 min | ★★ |
| 2 | [Signals & Queries](lab-2-signals-and-queries.md) | 45 min | ★★ |
| 3 | [Updates](lab-3-updates.md) | 40 min | ★★ |
| 4 | [Schedules](lab-4-schedules.md) | 35 min | ★★ |

Labs 2 and 3 build on the **same** approval Workflow — do 2 before 3.

## Cross-cutting concept: determinism

Every lab today is subject to the determinism rules. Workflow code must not call
`Math.random()`, `System.currentTimeMillis()`, `Thread.sleep`, or do direct I/O.
Use `Workflow.currentTimeMillis()`, `Workflow.newRandom()`, `Workflow.sleep()`,
and push all I/O into Activities. Each lab's *Pitfalls* section flags where this
bites.

## Coming from Airflow?

| Airflow | Temporal | Lab |
|---|---|---|
| Parallel tasks / `TaskGroup` | `Async.function` + `Promise.allOf` | 1 |
| External trigger / sensor poke | Signal | 2 |
| Reading task state | Query | 2 |
| (no direct equivalent) | Update — request/response into a running Workflow | 3 |
| `schedule_interval` + `catchup` | Temporal Schedule + overlap policy | 4 |
