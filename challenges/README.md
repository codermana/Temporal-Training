# Hands-on Challenges

Lab instructions for the 6-day Temporal training. Each lab is a self-contained
challenge: a scenario, the starter code you need, the tasks to complete, and a
checklist to confirm you got it right. **The instructions deliberately do not
contain the finished implementation** — that is the work.

> Reference solutions exist for instructors in [`examples/runnable/`](../examples/runnable/).
> Resist peeking until you have a working attempt (or are truly stuck). Each lab
> ends with progressive *Hints* that get you unblocked without handing over the
> answer.

## Layout

```
challenges/
  day-01-foundations/     Local setup, first Workflow, reading history
  day-02-reliability/     Async/parallel, signals/queries, updates, schedules
  day-03-kafka/           Kafka↔Temporal pipeline, partition fan-out
  day-04-production/      Observability, unit testing, replay testing
  day-05-saga-spring/     Order saga, Spring Boot saga, capstone
  day-06-aws-containers/  Glue/S3/Step Functions migration, Docker, K8s, KEDA
```

Each `day-XX/` folder has a `README.md` (the day's lab list + required stack)
and one `lab-N-*.md` per hands-on exercise.

## How to work a lab

1. Read the **Scenario** and **Learning goals** at the top.
2. Bring up the **Prerequisites** stack listed for that day.
3. Create a working project from the **Starter code** section. Every lab tells
   you either to scaffold a fresh Maven module or gives the interfaces/stubs to
   paste. Starter code is intentionally incomplete — `// TODO` marks the gaps.
4. Work the **Tasks** in order.
5. Run the **Verification** steps and tick off the **Definition of done**.
6. If stuck, expand **Hints** one at a time. Then try **Stretch goals**.

## Conventions

- **Where you write code.** Keep your attempts out of `examples/runnable/`.
  Create a scratch area, e.g. `mkdir -p work/day-01` at the repo root (already
  covered by `.gitignore` patterns for build output — confirm before committing).
- **Java / SDK versions.** JDK 17+ (JDK 21 for the virtual-threads stretch on
  Day 4), Temporal Java SDK `1.32.1`. The dependency block is in
  [`outline/detailed.md`](../outline/detailed.md#java-sdk-dependency-reference).
- **Running things.** The repo's `Makefile` wraps everything. `make help` lists
  targets. The three-terminal pattern is constant across days:

  | Terminal | Command | Notes |
  |---|---|---|
  | 1 | `make temporal` | Temporal dev server + Web UI on <http://127.0.0.1:8233> |
  | 2 | `make stack-kafka` / `stack-obs` / `stack-aws` | Only on days that need a stack |
  | 3 | your lab | `mvn ...` or `make run-<name>` |

  **The `make` targets are shortcuts, not magic.** Each one just runs a `temporal`,
  `mvn`, or `docker compose` command you could type yourself — and you should know
  which, because production won't have this Makefile. Every lab now includes an
  *"Under the hood"* callout next to its `make` commands revealing the real
  invocation (and the `TEMPORAL_*` env vars that drive connections). The table
  below decodes the targets you'll meet most; the source of truth is the
  [`Makefile`](../Makefile) and [`scripts/`](../scripts/).

  <details><summary>Make targets, decoded</summary>

  | Target | What it actually runs |
  |---|---|
  | `make temporal` | `temporal server start-dev --ip 127.0.0.1 --port 7233 --ui-port 8233 --metrics-port 7234` |
  | `make temporal-persistent` | same, plus `--db-filename .temporal/dev-server.db` (survives restarts) |
  | `make stack-temporal` | `docker compose -f docker/compose.temporal.yml up -d` (auto-setup + PostgreSQL + UI) |
  | `make stack-kafka` | `docker compose -f docker/compose.kafka.yml up -d` (Kafka KRaft on :9092) |
  | `make stack-obs` | `docker compose -f docker/compose.observability.yml up -d` (Prometheus :9091 + Grafana :3000) |
  | `make stack-aws` | `docker compose -f docker/compose.localstack.yml up -d` (LocalStack on :4566) |
  | `make stack-down` | `docker compose -f <files> down -v` (tears down + removes volumes) |
  | `make run-<name>` | runs the lab's **Worker** (`cd examples/runnable/<module> && mvn -q compile exec:java`; see `scripts/run-example.sh`) |
  | `make run-<name>-starter` | runs the lab's standalone **starter** (client) in a second terminal, for the split labs (`hello`, `connect`, `async`, `approval`, `retries`, `child`, `continue`) |
  | `make run-connect` | the env-driven Worker — reads `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_API_KEY` / `TEMPORAL_TLS_CERT` / `TEMPORAL_TLS_KEY`; defaults to plaintext `127.0.0.1:7233` / `default` (start a Workflow with `make run-connect-starter`) |
  | `make run-testing`, `run-replay` | `mvn -q test` in the module (no server needed) |
  | `make start-workflow QUEUE=q ID=n` | `temporal workflow start --task-queue q --type ImportWorkflow --workflow-id importworkflow-n --input "..."` |
  | `make kafka-topic TOPIC=t` | `docker exec temporal-training-kafka .../kafka-topics.sh --bootstrap-server localhost:9092 --create --topic t ...` |
  | `make kind-up` | `kind create cluster` + `helm install keda kedacore/keda -n keda` |
  | `make kind-load` | `docker build -t temporal-transform-worker:dev ...` + `kind load docker-image ...` |
  | `make check` | `scripts/check-local.sh` (verifies Java, Maven, Temporal CLI) |

  </details>

- **Web UI first.** After every run, open the Web UI and read the Event
  History. Most "why didn't it work" questions are answered there.
- **Audience framing.** Labs are tagged with the mental model they replace:
  `[airflow]`, `[kafka]`, `[aws]`, `[containers]`. If you come from that world,
  the *"Coming from…"* callout maps the old concept to the Temporal one.

## Difficulty & time

Each lab header lists an estimated time and a difficulty
(★ intro · ★★ core · ★★★ stretch). Times assume the lecture for that topic has
already been delivered.

## Day index

| Day | Topic | Labs | Stack |
|-----|-------|------|-------|
| [1](day-01-foundations/) | Foundations | 3 | Temporal only |
| [2](day-02-reliability/) | Reliable workflows | 4 | Temporal only |
| [3](day-03-kafka/) | Kafka integration | 2 | `make stack-kafka` |
| [4](day-04-production/) | Production engineering | 3 | `make stack-obs` (lab 1 only) |
| [5](day-05-saga-spring/) | Saga + Spring Boot + capstone | 3 | Temporal only |
| [6](day-06-aws-containers/) | AWS migration + containers | 5 | `make stack-aws` + `make kind-up` |
