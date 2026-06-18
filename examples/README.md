# Temporal Examples (Java · Python · Go)

Small examples that line up with the training outline. Most directories are
explanation snippets: they are meant to be shown during lectures before students
turn the idea into a complete lab. Runnable mini-projects are included where the
outline calls for hands-on work.

The course is Java-first, but from **Day 2 onward** each example day is split
into per-language subfolders so students on other stacks can follow along:

```
examples/02-reliability/
  java/    io.temporal:temporal-sdk   (the canonical version)
  python/  temporalio                 (asyncio)
  go/      go.temporal.io/sdk
```

Runnable labs follow the same shape, `examples/runnable/<lab>/{java,python,go}/`,
each with its own build file (`pom.xml`, `pyproject.toml`, `go.mod`). The
snippets teach one concept and are intentionally incomplete (see **Format**);
the runnable labs are complete, build, and run against a local dev server.

## Running

See [Setup.md](../Setup.md) for macOS, Linux, and Windows setup.

List examples:

```bash
scripts/list-examples.sh
```

Show a teaching snippet (the path includes the language subfolder):

```bash
scripts/show-example.sh 02-reliability/java/heartbeat_long_activity.java
scripts/show-example.sh 02-reliability/go/heartbeat_long_activity.go
```

Run a runnable example, Java by default, or pass a language:

```bash
scripts/start-temporal.sh
scripts/run-example.sh hello            # Java
scripts/run-example.sh async python     # same lab, Python SDK
scripts/run-example.sh async go         # same lab, Go SDK
```

## Map to the Outline

- `01-foundations`: Airflow DAG versus Temporal Workflow, core primitives, replay-safe code.
- `02-reliability`: Async Activity calls, partial failure, cancellation, retries, heartbeats, time.
- `03-interactions`: Signals, Queries, Updates, Schedules, workflow timeouts, Child Workflows.
- `04-kafka`: Kafka bridge, Kafka Activity, producer Activity, partition fan-out, DLQ routing.
- `05-production`: Versioning, worker sizing, observability, namespaces, replay testing.
- `06-saga-spring`: Saga compensation, sync/async Saga APIs, choreography bridge, Spring bean wiring, continue-as-new.
- `07-aws-containers`: AWS primitive mapping, Glue wrapper, S3 references, Docker, Kubernetes, KEDA.
- `08-ai-ml`: Temporal for AI — durable agent loop (LLM + tools as Activities, Signals for turns), MCP tools backed by Workflows, and an ML training pipeline. See `08-ai-ml/notes.md`.
- `runnable`: Complete mini-projects for labs and live demos, including a choreography participant demo.

## Format

Snippet files intentionally optimize for teaching clarity over complete
application structure. They often omit package declarations, imports, or concrete
infrastructure setup so the important Temporal concept stays visible. This holds
in every language: the Python and Go snippets reference activity functions that
aren't defined in the file, the same way the Java snippets assume their
interfaces exist. The **runnable** projects, by contrast, are complete and
compile/run as-is.
