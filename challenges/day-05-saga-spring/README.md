# Day 5 — Saga pattern, Spring Boot & capstone

Today ties everything together: a transactional order **saga** with
compensation, wired into **Spring Boot**, and a **capstone** where you redesign a
Kafka-triggered Airflow DAG as a Temporal Saga end-to-end.

## Required stack

Temporal dev server only (the capstone optionally adds Kafka if your team wires a
real listener):

```bash
make temporal
# optional for capstone Kafka trigger:
make stack-kafka
```

## Labs

| # | Lab | Time | Difficulty |
|---|-----|------|-----------|
| 1 | [Order saga walkthrough](lab-1-order-saga-walkthrough.md) | 60 min | ★★ |
| 2 | [Saga in Spring Boot](lab-2-saga-spring-boot.md) | 70 min | ★★★ |
| 3 | [Capstone — design & build a saga](lab-3-capstone.md) | 90 min | ★★★ |

Lab 1 establishes the saga; Lab 2 productionizes it in Spring; Lab 3 is open-ended.

## The saga mental model

A saga is a sequence of local transactions, each with a **compensating** action
that undoes it. If step N fails, you run the compensations for steps N-1 … 1 in
reverse. Temporal makes this natural: the Workflow is the orchestrator, the
forward and compensating steps are Activities, and durability means a crash
mid-saga resumes exactly where it left off.

```
authorizePayment ─▶ reserveInventory ─▶ ship
       │                   │              ✗ fails
   cancelPayment  ◀── restoreInventory ◀──┘   (compensate in reverse)
```

## Coming from Airflow / Kafka?

- **Orchestration vs choreography:** Temporal favors orchestration — one
  Workflow owns the sequence and the rollback, instead of services reacting to
  each other's events and hoping the compensation fires.
- **`on_failure_callback` cleanup → explicit compensation stack:** rollback is
  first-class code, not a best-effort hook.
