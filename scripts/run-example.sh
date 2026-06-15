#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  cat <<'EOF'
Usage: scripts/run-example.sh <example> [lang]

  lang is one of: java (default), python, go

Examples:
  scripts/run-example.sh hello
  scripts/run-example.sh connect      # env-driven: Docker (1.2b) or Cloud (1.2c)
  scripts/run-example.sh async        # Java (default)
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

Use scripts/list-examples.sh to see all examples.
EOF
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 2
fi

EXAMPLE="$1"
LANG_CHOICE="${2:-java}"
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
    if ! command -v python3 >/dev/null 2>&1; then
      echo "python3 is required. See Setup.md." >&2
      exit 1
    fi
    # Conventional entrypoints, in order of preference.
    ENTRY=""
    for cand in worker.py main.py starter.py run.py; do
      [[ -f "$cand" ]] && { ENTRY="$cand"; break; }
    done
    if [[ -z "$ENTRY" ]]; then
      echo "No worker.py/main.py entrypoint found in $DIR/python." >&2
      exit 1
    fi
    echo "Tip: pip install -r requirements.txt (ideally in a venv) before running." >&2
    python3 "$ENTRY"
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
    go run .
    ;;
  *)
    echo "Unknown lang: $LANG_CHOICE (use java, python, or go)" >&2
    exit 2
    ;;
esac
