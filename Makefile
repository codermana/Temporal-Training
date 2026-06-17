# Temporal Training - top-level Makefile
#
# Action targets only. All targets call the underlying shell scripts so the
# behavior is identical whether you invoke them via `make` or directly.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.ONESHELL:
.DEFAULT_GOAL := help

ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# ---------------------------------------------------------------------------
# Help (kept minimal; targets below are the real surface)
# ---------------------------------------------------------------------------

.PHONY: help
help: ## List targets
	@awk 'BEGIN {FS = ":.*##"} \
		/^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

.PHONY: setup-mac setup-mac-full setup-ubuntu setup-ubuntu-full check

setup-mac: ## Install the required tools on macOS (JDK, Maven, Temporal CLI)
	brew install openjdk maven temporal

setup-mac-full: setup-mac ## Install required + optional tools on macOS
	brew install docker kcat kind kubectl helm
	@command -v pipx >/dev/null 2>&1 || brew install pipx
	pipx install awscli-local || true

setup-ubuntu: ## Install the required tools on Ubuntu/Debian (uses sudo)
	sudo apt-get update
	sudo apt-get install -y openjdk-17-jdk maven curl
	@if ! command -v temporal >/dev/null 2>&1; then \
		echo ">> Installing Temporal CLI to ~/.temporalio/bin"; \
		curl -sSf https://temporal.download/cli.sh | sh; \
	fi
	@# Persist ~/.temporalio/bin on PATH (idempotent) for interactive shells.
	@LINE='export PATH="$$HOME/.temporalio/bin:$$PATH"'; \
	for RC in "$$HOME/.bashrc" "$$HOME/.profile"; do \
		if [ -f "$$RC" ] || [ "$$RC" = "$$HOME/.bashrc" ]; then \
			if ! grep -qsF '.temporalio/bin' "$$RC"; then \
				echo "$$LINE" >> "$$RC"; \
				echo ">> Added ~/.temporalio/bin to PATH in $$RC"; \
			fi; \
		fi; \
	done
	@echo ">> Open a new shell or run: export PATH=\"\$$HOME/.temporalio/bin:\$$PATH\""

setup-ubuntu-full: setup-ubuntu ## Install required + optional tools on Ubuntu/Debian (uses sudo)
	sudo apt-get install -y docker.io pipx
	@if ! docker compose version >/dev/null 2>&1; then \
		sudo apt-get install -y docker-compose-plugin || true; \
	fi
	@if ! docker compose version >/dev/null 2>&1; then \
		echo ">> Installing Docker Compose v2 plugin from GitHub releases"; \
		ARCH=$$(uname -m); \
		case "$$ARCH" in \
			x86_64|amd64) COMPOSE_ARCH=x86_64 ;; \
			aarch64|arm64) COMPOSE_ARCH=aarch64 ;; \
			*) echo "Unsupported CPU architecture for Docker Compose: $$ARCH" >&2; exit 1 ;; \
		esac; \
		sudo mkdir -p /usr/local/lib/docker/cli-plugins; \
		curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$$COMPOSE_ARCH" -o /tmp/docker-compose; \
		sudo install -m 0755 /tmp/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose; \
		rm -f /tmp/docker-compose; \
	fi
	@# kcat: package name is `kcat` on 22.04+, `kafkacat` on older releases.
	@sudo apt-get install -y kcat 2>/dev/null || sudo apt-get install -y kafkacat
	@# Add the current user to the docker group (effective after re-login).
	@if ! id -nG "$$USER" | tr ' ' '\n' | grep -qx docker; then \
		sudo usermod -aG docker "$$USER"; \
		echo ">> Added $$USER to the docker group. Log out + back in for it to take effect."; \
	fi
	@if ! command -v kubectl >/dev/null 2>&1; then \
		echo ">> Installing kubectl"; \
		KVER=$$(curl -sL https://dl.k8s.io/release/stable.txt); \
		curl -sLo /tmp/kubectl "https://dl.k8s.io/release/$$KVER/bin/linux/amd64/kubectl"; \
		sudo install -m 0755 /tmp/kubectl /usr/local/bin/kubectl; \
		rm -f /tmp/kubectl; \
	fi
	@if ! command -v kind >/dev/null 2>&1; then \
		echo ">> Installing kind"; \
		curl -sLo /tmp/kind https://kind.sigs.k8s.io/dl/v0.23.0/kind-linux-amd64; \
		sudo install -m 0755 /tmp/kind /usr/local/bin/kind; \
		rm -f /tmp/kind; \
	fi
	@if ! command -v helm >/dev/null 2>&1; then \
		echo ">> Installing helm"; \
		curl -sSf https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash; \
	fi
	@pipx install awscli-local || pipx upgrade awscli-local || true
	@pipx ensurepath || true

check: ## Verify required and optional tools
	scripts/check-local.sh

# ---------------------------------------------------------------------------
# Temporal server
# ---------------------------------------------------------------------------

.PHONY: temporal temporal-persistent ui

temporal: ## Start the Temporal dev server (foreground)
	scripts/start-temporal.sh

temporal-persistent: ## Start Temporal dev server with local persistent state
	scripts/start-temporal-persistent.sh

ui: ## Open the Temporal Web UI in the default browser
	@open http://127.0.0.1:8233 2>/dev/null || xdg-open http://127.0.0.1:8233 2>/dev/null || \
		echo "Open http://127.0.0.1:8233 manually"

# ---------------------------------------------------------------------------
# Docker stacks (Day 3 Kafka, Day 4 Observability, Day 6 LocalStack)
# ---------------------------------------------------------------------------

.PHONY: stack-temporal stack-kafka stack-obs stack-aws stack-all stack-down stack-status grafana prometheus localstack

stack-temporal: ## Day 1 variant: Temporal cluster + PostgreSQL + UI on :7233/:8233 (stop 'make temporal' first)
	scripts/start-stack.sh temporal up

stack-kafka: ## Day 3: Kafka KRaft broker on :9092
	scripts/start-stack.sh kafka up

stack-obs: ## Day 4: Prometheus :9091 + Grafana :3000
	scripts/start-stack.sh obs up

stack-aws: ## Day 6 AM: LocalStack (S3/SQS/SNS/SSM/Glue) on :4566
	scripts/start-stack.sh aws up

stack-all: ## Bring up every docker stack
	scripts/start-stack.sh all up

stack-down: ## Tear down every docker stack (removes volumes)
	scripts/start-stack.sh all down

stack-status: ## docker compose ps across stacks
	scripts/start-stack.sh all status

grafana: ## Open Grafana in the default browser (admin/admin)
	@open http://127.0.0.1:3000 2>/dev/null || xdg-open http://127.0.0.1:3000 2>/dev/null || \
		echo "Open http://127.0.0.1:3000 manually"

prometheus: ## Open Prometheus in the default browser
	@open http://127.0.0.1:9091 2>/dev/null || xdg-open http://127.0.0.1:9091 2>/dev/null || \
		echo "Open http://127.0.0.1:9091 manually"

localstack: ## Show LocalStack health
	@curl -sf http://127.0.0.1:4566/_localstack/health | jq . 2>/dev/null || \
		curl -sf http://127.0.0.1:4566/_localstack/health

# ---------------------------------------------------------------------------
# Kubernetes (Day 6 PM)
# ---------------------------------------------------------------------------

.PHONY: kind-up kind-load kind-status kind-down

kind-up: ## Create kind cluster and install KEDA via Helm
	scripts/start-kind.sh up

kind-load: ## Build the Worker image and load it into kind
	scripts/start-kind.sh load

kind-status: ## Show kind cluster + KEDA + ScaledObject status
	scripts/start-kind.sh status

kind-down: ## Delete the kind cluster
	scripts/start-kind.sh down

# ---------------------------------------------------------------------------
# Examples - list / show / run
# ---------------------------------------------------------------------------

.PHONY: list show challenges

list: ## List every example (snippets + runnable projects)
	scripts/list-examples.sh

challenges: ## List the hands-on labs under challenges/
	@echo "Hands-on challenges (see challenges/README.md):"
	@echo
	@for day in challenges/day-*/; do \
		title=$$(awk 'NR==1{sub(/^# /,""); print; exit}' "$$day/README.md" 2>/dev/null); \
		echo "  $${title:-$$day}"; \
		for lab in "$$day"lab-*.md; do \
			[ -e "$$lab" ] || continue; \
			name=$$(awk 'NR==1{sub(/^# /,""); print; exit}' "$$lab"); \
			printf "    - %-44s %s\n" "$$(basename "$$lab")" "$$name"; \
		done; \
		echo; \
	done

show: ## Print an example file (FILE=02-reliability/heartbeat_long_activity.java)
	@if [ -z "$(FILE)" ]; then echo "Usage: make show FILE=<path-under-examples>"; exit 2; fi
	scripts/show-example.sh $(FILE)

.PHONY: run-hello run-connect run-async run-approval run-schedules run-kafka run-testing run-saga run-aws
.PHONY: run-retries run-child run-replay run-continue run-choreography
.PHONY: run-hello-starter run-connect-starter run-async-starter run-approval-starter
.PHONY: run-retries-starter run-child-starter run-continue-starter run-choreography-starter

# Split labs ship a standalone Worker and a standalone starter (client). The
# run-* target launches the long-lived Worker; the run-*-starter target starts
# one Workflow from a second terminal.

run-hello:     ## Day 1: Hello Temporal (Worker)
	scripts/run-example.sh hello
run-hello-starter:    ## Day 1: Hello Temporal (starter)
	scripts/run-example.sh hello java starter

run-connect:   ## Day 1 variant: env-driven connection Worker (Docker 1.2b / Cloud 1.2c)
	scripts/run-example.sh connect
run-connect-starter:  ## Day 1 variant: env-driven connection starter
	scripts/run-example.sh connect java starter

run-async:     ## Day 2 AM: async + parallel activities (Worker)
	scripts/run-example.sh async
run-async-starter:    ## Day 2 AM: async + parallel activities (starter)
	scripts/run-example.sh async java starter

run-approval:  ## Day 2 PM: signals/queries/updates (Worker)
	scripts/run-example.sh approval
run-approval-starter: ## Day 2 PM: signals/queries/updates (starter)
	scripts/run-example.sh approval java starter

run-schedules: ## Day 2 PM: Schedules
	scripts/run-example.sh schedules

run-kafka:     ## Day 3: Kafka bridge (needs stack-kafka)
	scripts/run-example.sh kafka

run-testing:   ## Day 4: in-process Workflow tests (no server needed)
	scripts/run-example.sh testing

run-saga:      ## Day 5: Saga (Worker; start Workflows from the Temporal CLI)
	scripts/run-example.sh saga

run-aws:       ## Day 6: Import Worker (needs stack-aws + temporal)
	scripts/run-example.sh aws

run-retries:   ## Day 2: retries + heartbeat (Worker, deep-dive runnable)
	scripts/run-example.sh retries
run-retries-starter:  ## Day 2: retries + heartbeat (starter)
	scripts/run-example.sh retries java starter

run-child:     ## Day 2: child Workflows (Worker, deep-dive runnable)
	scripts/run-example.sh child
run-child-starter:    ## Day 2: child Workflows (starter)
	scripts/run-example.sh child java starter

run-replay:    ## Day 1/4: determinism replay test (no server needed)
	scripts/run-example.sh replay

run-continue:  ## Day 5: continue-as-new (Worker, deep-dive runnable)
	scripts/run-example.sh continue
run-continue-starter: ## Day 5: continue-as-new (starter)
	scripts/run-example.sh continue java starter

run-choreography: ## Day 5: choreography participant (Worker)
	scripts/run-example.sh choreography
run-choreography-starter: ## Day 5: choreography participant (starter)
	scripts/run-example.sh choreography java starter

# ---------------------------------------------------------------------------
# Per-day bundles - bring everything required for that day up / down
# ---------------------------------------------------------------------------

.PHONY: day-1-up day-2-up day-3-up day-4-up day-5-up day-6-up
.PHONY: day-3-down day-4-down day-6-down day-down

day-1-up: ## Day 1 stack: nothing extra beyond Temporal
	@echo "Day 1 needs only 'make temporal' in another terminal."

day-2-up: ## Day 2 stack: nothing extra beyond Temporal
	@echo "Day 2 needs only 'make temporal' in another terminal."

day-3-up: stack-kafka ## Day 3 stack: Kafka
	@echo "Day 3 ready. Also run 'make temporal' in another terminal."

day-4-up: stack-obs ## Day 4 stack: Prometheus + Grafana
	@echo "Day 4 ready. Also run 'make temporal' in another terminal."

day-5-up: ## Day 5 stack: nothing extra beyond Temporal
	@echo "Day 5 needs only 'make temporal' in another terminal."

day-6-up: stack-aws kind-up kind-load ## Day 6 stack: LocalStack + kind + KEDA + Worker image
	@echo "Day 6 ready. Also run 'make temporal' in another terminal."

day-3-down: ## Tear down Day 3 stack
	scripts/start-stack.sh kafka down

day-4-down: ## Tear down Day 4 stack
	scripts/start-stack.sh obs down

day-6-down: ## Tear down Day 6 stack (LocalStack + kind)
	scripts/start-stack.sh aws down
	scripts/start-kind.sh down

day-down: stack-down kind-down ## Tear down every day-related stack

# ---------------------------------------------------------------------------
# Workflow helpers
# ---------------------------------------------------------------------------

.PHONY: start-workflow load-transform

start-workflow: ## Start a Workflow (QUEUE=transform ID=1 [TYPE=ImportWorkflow] [INPUT=...])
	@if [ -z "$(QUEUE)" ] || [ -z "$(ID)" ]; then \
		echo "Usage: make start-workflow QUEUE=<task-queue> ID=<suffix> [TYPE=<type>] [INPUT=<arg>]"; \
		exit 2; \
	fi
	scripts/start-workflow.sh "$(QUEUE)" "$(ID)" "$(TYPE)" "$(INPUT)"

load-transform: ## Start N=200 Workflows on the transform queue (KEDA demo)
	N=$${N:-200}
	for i in $$(seq 1 $$N); do scripts/start-workflow.sh transform "$$i"; done

# ---------------------------------------------------------------------------
# Kafka helpers (Day 3)
# ---------------------------------------------------------------------------

.PHONY: kafka-topic kafka-produce kafka-consume

kafka-topic: ## Create a Kafka topic (TOPIC=orders [PARTITIONS=4])
	@if [ -z "$(TOPIC)" ]; then echo "Usage: make kafka-topic TOPIC=<name> [PARTITIONS=4]"; exit 2; fi
	docker exec temporal-training-kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --create --if-not-exists \
		--topic $(TOPIC) --partitions $${PARTITIONS:-4} --replication-factor 1

kafka-produce: ## Produce a record (TOPIC=orders KEY=k VALUE=v)
	@if [ -z "$(TOPIC)" ]; then echo "Usage: make kafka-produce TOPIC=<t> KEY=<k> VALUE=<v>"; exit 2; fi
	@printf '%s:%s\n' "$${KEY:-key}" "$${VALUE:-value}" | \
		kcat -b localhost:9092 -t $(TOPIC) -K: -P

kafka-consume: ## Tail a topic (TOPIC=order-outcomes)
	@if [ -z "$(TOPIC)" ]; then echo "Usage: make kafka-consume TOPIC=<t>"; exit 2; fi
	kcat -b localhost:9092 -t $(TOPIC) -C -o end -f 'key=%k value=%s\n'

# ---------------------------------------------------------------------------
# LocalStack helpers (Day 6)
# ---------------------------------------------------------------------------

.PHONY: aws-init aws-buckets aws-resources

aws-init: ## Create all LocalStack resources used by Day 6 AM labs (S3/SQS/SNS/SSM)
	# Lab 1-3: import buckets
	awslocal s3 mb s3://imports-incoming  || true
	awslocal s3 mb s3://imports-validated || true
	awslocal s3 mb s3://imports-output    || true
	# Lab 6: SQS event-trigger queue (the EventBridge->Lambda->StartExecution stand-in)
	awslocal sqs create-queue --queue-name imports-events || true
	# Lab 7: SNS completion topic + an SQS subscriber to verify fan-out
	awslocal sns create-topic --name imports-complete || true
	awslocal sqs create-queue --queue-name imports-complete-sub || true
	# Lab 8: Worker config tree in SSM Parameter Store (+ one SecureString secret)
	awslocal ssm put-parameter --name /temporal-training/worker/temporal-address --value 127.0.0.1:7233 --type String --overwrite || true
	awslocal ssm put-parameter --name /temporal-training/worker/namespace        --value default        --type String --overwrite || true
	awslocal ssm put-parameter --name /temporal-training/worker/task-queue        --value transform      --type String --overwrite || true
	awslocal ssm put-parameter --name /temporal-training/worker/api-key           --value local-dev-secret --type SecureString --overwrite || true

aws-buckets: ## List LocalStack S3 buckets
	awslocal s3 ls

aws-resources: ## List the Day 6 LocalStack resources (buckets, queues, topics, params)
	@echo "== S3 buckets =="       && awslocal s3 ls
	@echo "== SQS queues =="       && awslocal sqs list-queues
	@echo "== SNS topics =="       && awslocal sns list-topics
	@echo "== SSM parameters ==" && awslocal ssm get-parameters-by-path --path /temporal-training/worker/ --with-decryption

# ---------------------------------------------------------------------------
# Slides (Marp)
# ---------------------------------------------------------------------------

.PHONY: slides slides-deps slides-css slides-why slides-fundamentals slides-html slides-pdf slides-pptx slides-lint

slides-deps: ## Install Marp CLI for a deck (DECK=why-temporal|temporal-fundamentals)
	@DECK=$${DECK:-why-temporal}
	@if [ ! -d "slides/$$DECK" ]; then echo "Unknown deck: $$DECK"; exit 2; fi
	cd "slides/$$DECK" && npm install

slides-css: ## Compile the shared theme slides/themes/base.scss -> base.css
	@if [ ! -d "slides/temporal-fundamentals/node_modules" ]; then \
		echo ">> Installing slide tooling"; \
		cd "slides/temporal-fundamentals" && npm install; \
	fi
	cd "slides/temporal-fundamentals" && npm run css

slides: ## Preview a Marp deck (DECK=why-temporal|temporal-fundamentals [PORT=8080])
	@DECK=$${DECK:-why-temporal}
	@PORT=$${PORT:-8080}
	@if [ ! -d "slides/$$DECK" ]; then echo "Unknown deck: $$DECK"; exit 2; fi
	@if [ ! -d "slides/$$DECK/node_modules" ]; then \
		echo ">> Installing Marp CLI for $$DECK"; \
		cd "slides/$$DECK" && npm install; \
	fi
	@echo ">> Previewing $$DECK at http://localhost:$$PORT"
	cd "slides/$$DECK" && PORT=$$PORT npm run preview

slides-why: ## Preview Why Temporal at :8080
	$(MAKE) slides DECK=why-temporal PORT=8080

slides-fundamentals: ## Preview Temporal Fundamentals at :8081
	$(MAKE) slides DECK=temporal-fundamentals PORT=8081

slides-html: ## Export a deck to HTML (DECK=<name>) -> slides/<deck>/dist/index.html
	@DECK=$${DECK:-why-temporal}
	cd "slides/$$DECK" && npm run html

slides-pdf: ## Export a deck to PDF (DECK=<name>) -> slides/<deck>/dist/slides.pdf
	@DECK=$${DECK:-why-temporal}
	cd "slides/$$DECK" && npm run pdf

slides-pptx: ## Export a deck to PPTX (DECK=<name>) -> slides/<deck>/dist/slides.pptx
	@DECK=$${DECK:-why-temporal}
	cd "slides/$$DECK" && npm run pptx

slides-lint: ## Flag slides whose content overflows the slide box (DECK=<name> for one deck)
	node scripts/check-slides.mjs $(DECK)

# ---------------------------------------------------------------------------
# Build / test
# ---------------------------------------------------------------------------

.PHONY: test build clean

test: ## Compile every runnable Maven project
	scripts/test-runnable.sh

build: ## Package every runnable project (produces target/*.jar)
	@for d in examples/runnable/*/; do \
		echo "==> $$d"; \
		(cd "$$d" && mvn -q -DskipTests package) || exit 1; \
	done

clean: ## mvn clean for every runnable project
	@for d in examples/runnable/*/; do \
		(cd "$$d" && mvn -q clean) || exit 1; \
	done
