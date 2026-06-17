# Day 1: Foundations

Goal for the day: get a local Temporal environment running, write and run your
first Workflow in Java, and learn to read the Event History, the artifact every
later day depends on.

## Required stack

Temporal dev server only. In a dedicated terminal:

```bash
make temporal      # Frontend/History/Matching + Web UI + SQLite, single binary
```

Web UI: <http://127.0.0.1:8233> · gRPC: `127.0.0.1:7233`

No Docker, Kafka, or AWS needed today.

## Labs

| # | Lab | Time | Difficulty |
|---|-----|------|-----------|
| 1 | [Local dev setup](lab-1-local-dev-setup.md) | 20 min | ★ |
| 2 | [Hello Temporal: your first Workflow](lab-2-hello-temporal.md) | 50 min | ★ |
| 2b | [Hello Temporal on a Dockerized cluster](lab-2-hello-temporal-docker.md) | 30 min | ★ |
| 2c | [Hello Temporal on Temporal Cloud](lab-2-hello-temporal-cloud.md) | 40 min | ★★ |
| 3 | [Reading the Event History](lab-3-reading-event-history.md) | 30 min | ★ |

Work them in order; lab 2 builds the Workflow that lab 3 inspects. Labs 2b and
2c are optional variants of lab 2 that build on the **same** base: one
env-driven worker (`Connections.fromEnv()`, in
[`examples/runnable/01b-hello-temporal-anywhere`](../../examples/runnable/01b-hello-temporal-anywhere))
that picks its connection from the environment. 2b points it at a Dockerized
cluster (plaintext, no config); 2c points it at Temporal Cloud (TLS + auth). Same
worker, same helper; only the environment differs. Skip 2c if you don't have a
Cloud namespace.

## Coming from Airflow?

| Airflow | Temporal | Where you meet it |
|---|---|---|
| DAG | Workflow (`@WorkflowInterface`) | Lab 2 |
| Operator / task | Activity (`@ActivityInterface`) | Lab 2 |
| Executor / worker | Worker (`WorkerFactory` -> `Worker`) | Lab 2 |
| Scheduler trigger | `WorkflowClient.start(...)` | Lab 2 |
| Task instance log / XCom | Event History | Lab 3 |
| Web UI grid/graph | Temporal Web UI | All |
