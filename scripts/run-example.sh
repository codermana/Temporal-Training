#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  command "$@"
}

usage() {
  cat <<'EOF'
Usage: scripts/run-example.sh <example> [lang] [role]

  lang is one of: java (default), python, go
  role is one of: worker (default), starter

Most labs now ship a standalone Worker and a standalone starter (client). Run
the Worker in one terminal, then the starter in another:

  scripts/run-example.sh async go worker    # terminal 1: long-lived Worker
  scripts/run-example.sh async go starter   # terminal 2: starts one Workflow

role is ignored for labs that are not split (e.g. saga/aws run the Worker and
are driven from the Temporal CLI; testing/replay run a test suite).

Examples:
  scripts/run-example.sh hello
  scripts/run-example.sh connect      # env-driven: Docker (1.2b) or Cloud (1.2c)
  scripts/run-example.sh async        # Java (default), Worker role
  scripts/run-example.sh async python # same lab, Python SDK
  scripts/run-example.sh async go     # same lab, Go SDK
  scripts/run-example.sh approval
  scripts/run-example.sh schedules
  scripts/run-example.sh testing
  scripts/run-example.sh saga
  scripts/run-example.sh retries
  scripts/run-example.sh child
  scripts/run-example.sh replay
  scripts/run-example.sh continue
  scripts/run-example.sh choreography
  scripts/run-example.sh wordcount
  scripts/run-example.sh routing      # one Workflow, Activities on separate pools
  scripts/run-example.sh spring       # Temporal Spring Boot starter, REST on :8080 (Java only)
  scripts/run-example.sh spring-glue  # Spring Boot supervising a local stitch job (Glue stand-in), REST on :8080 (Java only)
  scripts/run-example.sh import       # plain-SDK Import Worker: real S3/SNS/SSM + SQS bridge (Java only)

Use scripts/list-examples.sh to see all examples.
EOF
}

if [[ $# -lt 1 || $# -gt 3 ]]; then
  usage
  exit 2
fi

EXAMPLE="$1"
LANG_CHOICE="${2:-java}"
ROLE="${3:-worker}"
case "$ROLE" in
  worker|starter) ;;
  *)
    echo "Unknown role: $ROLE (use worker or starter)" >&2
    exit 2
    ;;
esac
DIR=""
MODE="exec"
MAIN_CLASS=""
NEEDS_TEMPORAL="yes"

case "$EXAMPLE" in
  hello|01|01-hello-temporal)
    DIR="examples/runnable/01-hello-temporal"
    ;;
  connect|anywhere|01b|01b-hello-temporal-anywhere)
    # Env-driven connection (Lab 1.2b Docker / 1.2c Cloud). Skip the local :7233
    # precheck — TEMPORAL_ADDRESS may point at Docker or Cloud, not localhost.
    DIR="examples/runnable/01b-hello-temporal-anywhere"
    NEEDS_TEMPORAL="no"
    ;;
  async|parallel|02|02-async-parallel-activities)
    DIR="examples/runnable/02-async-parallel-activities"
    ;;
  approval|signals|updates|03|03-signals-queries-updates)
    DIR="examples/runnable/03-signals-queries-updates"
    ;;
  schedules|04|04-schedules)
    DIR="examples/runnable/04-schedules"
    MODE="compile"
    MAIN_CLASS="training.temporal.schedules.CreateSchedule"
    ;;
  kafka|05|05-kafka-bridge)
    DIR="examples/runnable/05-kafka-bridge"
    MODE="exec"
    MAIN_CLASS="training.temporal.kafka.KafkaWorker"
    ;;
  testing|test|06|06-testing)
    DIR="examples/runnable/06-testing"
    MODE="test"
    NEEDS_TEMPORAL="no"
    ;;
  saga|07|07-saga)
    DIR="examples/runnable/07-saga"
    MODE="exec"
    MAIN_CLASS="training.temporal.saga.SagaWorker"
    ;;
  aws|containers|transform|08|08-aws-containers)
    DIR="examples/runnable/08-aws-containers"
    MODE="exec"
    MAIN_CLASS="training.temporal.aws.WorkerMain"
    ;;
  retries|heartbeat|heartbeats|09|09-retries-heartbeats)
    DIR="examples/runnable/09-retries-heartbeats"
    ;;
  child|children|10|10-child-workflows)
    DIR="examples/runnable/10-child-workflows"
    ;;
  replay|determinism|11|11-determinism-replay)
    DIR="examples/runnable/11-determinism-replay"
    MODE="test"
    NEEDS_TEMPORAL="no"
    ;;
  continue|continueasnew|can|12|12-continue-as-new)
    DIR="examples/runnable/12-continue-as-new"
    ;;
  choreography|choreo|13|13-choreography)
    DIR="examples/runnable/13-choreography"
    ;;
  wordcount|word-count|fanout|14|14-word-count-fanout)
    DIR="examples/runnable/14-word-count-fanout"
    ;;
  routing|task-queue-routing|taskqueue|15|15-task-queue-routing)
    DIR="examples/runnable/15-task-queue-routing"
    ;;
  spring|springboot|spring-boot|16|16-spring-boot)
    # Single Spring Boot process: the starter stands up the Worker and the app
    # serves REST on :8080. Not Worker/starter split — role is ignored. Java only.
    DIR="examples/runnable/16-spring-boot"
    MODE="spring"
    ;;
  spring-glue|glue-spring|17|17-spring-glue-pipeline)
    # Spring Boot service that orchestrates a (faked) AWS Glue job: an SQS trigger
    # bridge + a REST front door drive a validate-S3 -> Glue -> SNS-notify
    # Workflow. One process on :8080. Needs LocalStack (make stack-aws) with the
    # demo resources seeded (scripts/seed-glue-demo.sh up). Java only.
    DIR="examples/runnable/17-spring-glue-pipeline"
    MODE="spring"
    ;;
  import|import-pipeline|aws-import|18|18-aws-import-pipeline)
    # Plain-SDK Java Worker for the Day-6 import pipeline: validate -> transform
    # (supervised job) -> load -> SNS-notify, doing REAL LocalStack S3/SNS/SSM
    # work, plus the SQS trigger bridge in the same process. Needs LocalStack
    # (make stack-aws) seeded via scripts/seed-import-demo.sh up. Java only.
    DIR="examples/runnable/18-aws-import-pipeline"
    MODE="exec"
    MAIN_CLASS="training.temporal.aws.WorkerMain"
    ;;
  *)
    echo "Unknown runnable example: $EXAMPLE" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ "$NEEDS_TEMPORAL" == "yes" ]]; then
  if ! nc -z 127.0.0.1 7233 >/dev/null 2>&1; then
    echo "Temporal is not reachable at 127.0.0.1:7233." >&2
    echo "Start it in another terminal with: scripts/start-temporal.sh" >&2
    exit 1
  fi
fi

case "$LANG_CHOICE" in
  java)
    # Migrated labs keep the Maven project under java/; older labs still have
    # pom.xml at the lab root. Prefer java/ when it exists.
    if [[ -d "$ROOT_DIR/$DIR/java" ]]; then
      cd "$ROOT_DIR/$DIR/java"
    else
      cd "$ROOT_DIR/$DIR"
    fi
    if ! command -v mvn >/dev/null 2>&1; then
      echo "Maven is required. See Setup.md." >&2
      exit 1
    fi
    # For split labs the Worker is the pom's default mainClass (a *Worker class);
    # the starter is the matching *Starter class. Derive it so `role starter`
    # works without per-lab config.
    if [[ "$ROLE" == "starter" ]]; then
      WORKER_CLASS="$MAIN_CLASS"
      if [[ -z "$WORKER_CLASS" ]]; then
        # The plugin's <mainClass> is usually just ${exec.mainClass}, which the
        # exec plugin can't resolve from the CLI (it cycles). Read the actual
        # *Worker class from the <exec.mainClass> property first; fall back to a
        # literal <mainClass> for older poms that inline the class.
        WORKER_CLASS="$(sed -n 's:.*<exec.mainClass>\(.*\)</exec.mainClass>.*:\1:p' pom.xml | head -1)"
        if [[ -z "$WORKER_CLASS" ]]; then
          WORKER_CLASS="$(sed -n 's:.*<mainClass>\(.*\)</mainClass>.*:\1:p' pom.xml | head -1)"
        fi
      fi
      MAIN_CLASS="${WORKER_CLASS/%Worker/Starter}"
    fi
    case "$MODE" in
      exec)
        if [[ -n "$MAIN_CLASS" ]]; then
          run mvn -q compile exec:java -Dexec.mainClass="$MAIN_CLASS"
        else
          run mvn -q compile exec:java
        fi
        ;;
      spring)
        # Spring Boot 3.3 targets Java 17-21. If the default `java` is newer,
        # transparently pin JDK 17 for this run so it works out of the box.
        JAVA_MAJOR="$(java -version 2>&1 | sed -n 's/.*version "\([0-9][0-9]*\).*/\1/p' | head -1)"
        if [[ -n "$JAVA_MAJOR" && "$JAVA_MAJOR" -gt 21 ]]; then
          if J17="$(/usr/libexec/java_home -v 17 2>/dev/null)"; then
            echo "Default Java is $JAVA_MAJOR; pinning JDK 17 for Spring Boot." >&2
            export JAVA_HOME="$J17"
          else
            echo "Warning: default Java is $JAVA_MAJOR and no JDK 17 found." >&2
            echo "Spring Boot 3.3 may not run on Java $JAVA_MAJOR. Install JDK 17." >&2
          fi
        fi
        run mvn -q -DskipTests spring-boot:run
        ;;
      test)
        run mvn -q test
        ;;
      compile)
        if [[ -n "$MAIN_CLASS" ]]; then
          run mvn -q compile exec:java -Dexec.mainClass="$MAIN_CLASS"
        else
          run mvn -q -DskipTests compile
        fi
        ;;
    esac
    ;;
  python)
    PDIR="$ROOT_DIR/$DIR/python"
    if [[ ! -d "$PDIR" ]]; then
      echo "No Python version of '$EXAMPLE' yet (expected $DIR/python)." >&2
      exit 1
    fi
    cd "$PDIR"
    # Conventional entrypoints, in order of preference. For split labs, prefer
    # the entry that matches the requested role (worker.py / starter.py).
    ENTRY=""
    if [[ "$ROLE" == "starter" && -f "starter.py" ]]; then
      ENTRY="starter.py"
    else
      for cand in worker.py main.py starter.py run.py; do
        [[ -f "$cand" ]] && { ENTRY="$cand"; break; }
      done
    fi
    if [[ -z "$ENTRY" ]]; then
      echo "No worker.py/main.py entrypoint found in $DIR/python." >&2
      exit 1
    fi
    # uv reads pyproject.toml, provisions an isolated env, and runs — no manual
    # venv/pip. Falls back to plain python3 if uv isn't installed.
    if command -v uv >/dev/null 2>&1; then
      run uv run "$ENTRY"
    elif command -v python3 >/dev/null 2>&1; then
      echo "uv not found; falling back to system python3 (install uv: https://docs.astral.sh/uv/)." >&2
      echo "Tip: create a venv and 'pip install .' from $DIR/python first." >&2
      run python3 "$ENTRY"
    else
      echo "uv (preferred) or python3 is required. See Setup.md." >&2
      exit 1
    fi
    ;;
  go)
    GDIR="$ROOT_DIR/$DIR/go"
    if [[ ! -d "$GDIR" ]]; then
      echo "No Go version of '$EXAMPLE' yet (expected $DIR/go)." >&2
      exit 1
    fi
    cd "$GDIR"
    if ! command -v go >/dev/null 2>&1; then
      echo "Go toolchain is required. See Setup.md." >&2
      exit 1
    fi
    # Split labs have ./worker and ./starter command dirs; older single-binary
    # labs keep main at the module root (go run .).
    if [[ -d "$ROLE" ]]; then
      run go run "./$ROLE"
    else
      run go run .
    fi
    ;;
  *)
    echo "Unknown lang: $LANG_CHOICE (use java, python, or go)" >&2
    exit 2
    ;;
esac
