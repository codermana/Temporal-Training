#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
        WORKER_CLASS="$(sed -n 's:.*<mainClass>\(.*\)</mainClass>.*:\1:p' pom.xml | head -1)"
      fi
      MAIN_CLASS="${WORKER_CLASS/%Worker/Starter}"
    fi
    case "$MODE" in
      exec)
        if [[ -n "$MAIN_CLASS" ]]; then
          mvn -q compile exec:java -Dexec.mainClass="$MAIN_CLASS"
        else
          mvn -q compile exec:java
        fi
        ;;
      test)
        mvn -q test
        ;;
      compile)
        if [[ -n "$MAIN_CLASS" ]]; then
          mvn -q compile exec:java -Dexec.mainClass="$MAIN_CLASS"
        else
          mvn -q -DskipTests compile
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
      uv run "$ENTRY"
    elif command -v python3 >/dev/null 2>&1; then
      echo "uv not found; falling back to system python3 (install uv: https://docs.astral.sh/uv/)." >&2
      echo "Tip: create a venv and 'pip install .' from $DIR/python first." >&2
      python3 "$ENTRY"
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
      go run "./$ROLE"
    else
      go run .
    fi
    ;;
  *)
    echo "Unknown lang: $LANG_CHOICE (use java, python, or go)" >&2
    exit 2
    ;;
esac
