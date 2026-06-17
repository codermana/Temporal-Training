# 16 · Basic Temporal Spring Boot (the starter)

A minimal, runnable Spring Boot service that uses **`temporal-spring-boot-starter`**
— the production-preferred way to wire Temporal into Spring. Day 5's slides walk
the *manual* `@Configuration` (three beans: stubs → client → factory) so the
plumbing is visible; **this project is what that plumbing automates.**

> Java only — the starter is a Java/Spring artifact. The saga and other labs ship
> in three SDKs; this one does not.

## What the starter does for you

There is **no `@Configuration` class** in this project. Everything is in
[`application.yml`](java/src/main/resources/application.yml):

- builds the `WorkflowClient` from the `connection` / `namespace` block,
- scans `workers-auto-discovery.packages` for `@WorkflowImpl` / `@ActivityImpl`,
- starts a Worker for each task queue those annotations declare,
- binds Worker **start / graceful-shutdown** to the Spring application lifecycle.

You write three things and inject one:

| Piece | File | Note |
| --- | --- | --- |
| Workflow impl | `GreetingWorkflowImpl` | `@WorkflowImpl(taskQueues = "greetings")` — auto-registered |
| Activity impl | `GreetingActivitiesImpl` | `@Component` **and** `@ActivityImpl` — a Spring bean with DI |
| REST front door | `GreetingController` | injects the auto-configured `WorkflowClient` |
| App | `SpringBootApp` | plain `@SpringBootApplication`, no Temporal config |

## Run it

Needs a Temporal dev server on `127.0.0.1:7233` (`scripts/start-temporal.sh` or
`make temporal`). Then, from the repo root:

```bash
make run-spring
# or: scripts/run-example.sh spring
```

This is a single process (the Spring app *is* both the Worker and the client) —
not the Worker/starter split the other labs use. It listens on `:8080`.

### Drive it over HTTP

```bash
# Start a Workflow and block for its result (synchronous request/response)
curl -s -X POST localhost:8080/greetings \
  -H 'content-type: application/json' \
  -d '{"name":"Ada"}'
# {"message":"Hello, Ada! (from a Temporal Activity that is a Spring bean)"}

# Query the same Workflow's status by its business id
curl -s localhost:8080/greetings/Ada
# {"message":"DONE"}
```

In the Web UI (`localhost:8233`) the execution is `greeting-Ada` on the
`greetings` task queue — started by an HTTP handler, executed by the Worker the
starter stood up.

> **JDK note for this repo's machine.** Spring Boot 3.3 targets Java 17–21. Build
> and run with JDK 17 if your default `java` is newer:
> `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn spring-boot:run`.

## Where this sits

- Manual wiring (the bean-by-bean version this replaces): `examples/06-saga-spring/spring_temporal_config.java`
- The full saga to wire in next: `examples/runnable/07-saga/`
- Driving Workflows from HTTP (Update) and Kafka (Signal): `examples/06-saga-spring/sync_saga_update.java`, `async_saga_signal.java`
