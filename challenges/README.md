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
