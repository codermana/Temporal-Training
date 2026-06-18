# Runnable Examples

These are complete mini-projects suitable for labs or live demos. The sibling
numbered directories contain smaller snippets for explanations.

Each lab ships in **three SDKs** under per-language subfolders: `java/`
(`io.temporal:temporal-sdk`), `python/` (`temporalio`), and `go/`
(`go.temporal.io/sdk`), with its own build file (`pom.xml`,
`pyproject.toml`, `go.mod`) and a `README.md` showing how to run all three.
Pick a language with the script's optional arg:

```bash
scripts/run-example.sh hello          # Java (default)
scripts/run-example.sh hello python   # Python SDK
scripts/run-example.sh hello go       # Go SDK
```

Most labs run the **Worker** and the **client (starter)** as two separate,
standalone processes, as you'd deploy them in production. Run the Worker in one
terminal and the starter in another with the optional `role` arg:

```bash
scripts/run-example.sh hello go worker     # terminal 1: long-lived Worker
scripts/run-example.sh hello go starter    # terminal 2: starts one Workflow
```

Per language the split is: Go `go run ./worker` / `go run ./starter` (shared defs
in a package at the module root); Python `worker.py` / `starter.py`; Java a
`*Worker` class / a `*Starter` class. A few labs aren't split: `04-schedules`
and `05-kafka-bridge` are driven differently, `07-saga`, `08-aws-containers` and
`18-aws-import-pipeline` run the Worker and are started from the Temporal CLI (or
the SQS bridge), and `06-testing` / `11-determinism-replay` run a test suite
instead. `16-spring-boot` and `17-spring-glue-pipeline` are single Spring Boot
processes (Java only): the starter hosts the Worker and the app serves REST on
`:8080`.

| Lab | Day | Topic |
| --- | --- | --- |
| `01-hello-temporal` | 1 | Workflow + Activity + Worker |
| `01b-hello-temporal-anywhere` | 1 | Env-driven connection (Docker / Cloud) |
| `02-async-parallel-activities` | 2 | Parallel fan-out / fan-in |
| `03-signals-queries-updates` | 2 | Signals, Queries, Updates |
| `04-schedules` | 2 | Schedules (calendar + overlap policy) |
| `05-kafka-bridge` | 3 | Kafka → Temporal signal bridge |
| `06-testing` | 4 | Workflow tests (time-skipping) |
| `07-saga` | 5 | Saga compensation |
| `08-aws-containers` | 6 | AWS / container worker |
| `09-retries-heartbeats` | 2 | Retries + heartbeats (deep dive) |
| `10-child-workflows` | 2 | Child Workflows (deep dive) |
| `11-determinism-replay` | 4 | Replay / non-determinism (deep dive) |
| `12-continue-as-new` | 5 | Continue-as-new (deep dive) |
| `13-choreography` | 5 | Event choreography with a durable Temporal participant |
| `14-word-count-fanout` | 2 | Word-count fan-out / fan-in |
| `15-task-queue-routing` | 5 | One Workflow, Activities on separate pools |
| `16-spring-boot` | 5 | Temporal Spring Boot starter (auto-config, REST-driven) |
| `17-spring-glue-pipeline` | 6 | Spring Boot orchestrating a Glue job end-to-end (S3 → SQS → SNS) |
| `18-aws-import-pipeline` | 6 | Runnable Day-6 morning labs (plain SDK): real S3 / SQS / SNS / SSM on LocalStack |
