#!/usr/bin/env bash
# Seed (and drive) the Day-6 runnable import pipeline on LocalStack.
#
# It stands in for "producer A" and the AWS control plane for
# examples/runnable/18-aws-import-pipeline: it creates the three import buckets
# (incoming/validated/output), the SQS trigger queue (Lab 6.6), the SNS
# completion topic + a subscribed SQS queue to prove fan-out (Lab 6.7), the SSM
# config tree + a SecureString secret (Lab 6.8), and lands an input CSV.
#
# Usage:
#   scripts/seed-import-demo.sh up                # create resources + land an input object
#   scripts/seed-import-demo.sh trigger [S3URI]   # drop a file-arrival message on the bus (Lab 6.6)
#   scripts/seed-import-demo.sh notify [N]         # read up to N messages from the SNS subscriber (Lab 6.7)
#   scripts/seed-import-demo.sh ls                 # list incoming / validated / output objects
#   scripts/seed-import-demo.sh down               # delete every demo resource
#
# Matches the defaults in Config.java and what `make aws-init` seeds.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT:-http://127.0.0.1:4566}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="000000000000"

BUCKET_INCOMING="${BUCKET_INCOMING:-imports-incoming}"
BUCKET_VALIDATED="${BUCKET_VALIDATED:-imports-validated}"
BUCKET_OUTPUT="${BUCKET_OUTPUT:-imports-output}"
INPUT_KEY="${INPUT_KEY:-orders.csv}"
INPUT_URI="s3://${BUCKET_INCOMING}/${INPUT_KEY}"

EVENTS_QUEUE="imports-events"
NOTIFY_TOPIC="imports-complete"
NOTIFY_SUB_QUEUE="imports-complete-sub"

EVENTS_QUEUE_URL="${ENDPOINT}/${ACCOUNT}/${EVENTS_QUEUE}"
NOTIFY_SUB_QUEUE_URL="${ENDPOINT}/${ACCOUNT}/${NOTIFY_SUB_QUEUE}"
NOTIFY_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:${NOTIFY_TOPIC}"
NOTIFY_SUB_QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:${NOTIFY_SUB_QUEUE}"
SSM_PREFIX="/temporal-training/worker"

# Prefer awslocal if installed; otherwise plain aws with the LocalStack endpoint
# and throwaway credentials (LocalStack ignores the values, but the SDK requires them).
if command -v awslocal >/dev/null 2>&1; then
  aws_() { awslocal --region "$REGION" "$@"; }
elif command -v aws >/dev/null 2>&1; then
  export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
  export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
  aws_() { aws --endpoint-url "$ENDPOINT" --region "$REGION" "$@"; }
else
  echo "Need the AWS CLI (or awslocal). See Setup.md." >&2
  exit 1
fi

require_localstack() {
  if ! curl -sf "${ENDPOINT}/_localstack/health" >/dev/null 2>&1; then
    echo "LocalStack is not reachable at ${ENDPOINT}. Start it with: make stack-aws" >&2
    exit 1
  fi
}

up() {
  require_localstack
  echo "Creating import buckets ..."
  aws_ s3 mb "s3://${BUCKET_INCOMING}"  >/dev/null 2>&1 || true
  aws_ s3 mb "s3://${BUCKET_VALIDATED}" >/dev/null 2>&1 || true
  aws_ s3 mb "s3://${BUCKET_OUTPUT}"    >/dev/null 2>&1 || true

  echo "Creating SQS trigger queue ${EVENTS_QUEUE} (the bus) ..."
  aws_ sqs create-queue --queue-name "$EVENTS_QUEUE" >/dev/null 2>&1 || true

  echo "Creating SNS topic ${NOTIFY_TOPIC} + subscriber queue ${NOTIFY_SUB_QUEUE} ..."
  aws_ sns create-topic --name "$NOTIFY_TOPIC" >/dev/null 2>&1 || true
  aws_ sqs create-queue --queue-name "$NOTIFY_SUB_QUEUE" >/dev/null 2>&1 || true
  # Subscribe the queue to the topic; RawMessageDelivery keeps the body as our JSON.
  aws_ sns subscribe --topic-arn "$NOTIFY_TOPIC_ARN" --protocol sqs \
    --notification-endpoint "$NOTIFY_SUB_QUEUE_ARN" \
    --attributes RawMessageDelivery=true >/dev/null 2>&1 || true

  echo "Seeding SSM config tree + SecureString secret ..."
  aws_ ssm put-parameter --name "${SSM_PREFIX}/temporal-address" --value "127.0.0.1:7233" --type String --overwrite >/dev/null
  aws_ ssm put-parameter --name "${SSM_PREFIX}/namespace"        --value "default"        --type String --overwrite >/dev/null
  aws_ ssm put-parameter --name "${SSM_PREFIX}/task-queue"       --value "transform"      --type String --overwrite >/dev/null
  aws_ ssm put-parameter --name "${SSM_PREFIX}/api-key"          --value "local-dev-secret" --type SecureString --overwrite >/dev/null

  echo "Landing input object ${INPUT_URI} ..."
  # A text column so the transform job's upper-casing is visibly demonstrated.
  printf 'id,sku,amount\n1,widget-a,10\n2,gadget-b,20\n3,gizmo-c,30\n' | aws_ s3 cp - "$INPUT_URI" >/dev/null

  echo
  echo "Seeded. Input object : ${INPUT_URI}"
  echo "        Trigger queue: ${EVENTS_QUEUE_URL}"
  echo "        Notify topic : ${NOTIFY_TOPIC_ARN}"
  echo "        Subscriber   : ${NOTIFY_SUB_QUEUE_URL}"
}

trigger() {
  require_localstack
  local uri="${1:-$INPUT_URI}"
  echo "Dropping file-arrival message for ${uri} on ${EVENTS_QUEUE} ..."
  aws_ sqs send-message --queue-url "$EVENTS_QUEUE_URL" \
    --message-body "{\"s3Uri\":\"${uri}\"}" >/dev/null
  echo "Sent."
}

notify() {
  require_localstack
  local n="${1:-5}"
  echo "Reading up to ${n} messages from the SNS subscriber (${NOTIFY_SUB_QUEUE}) ..."
  aws_ sqs receive-message --queue-url "$NOTIFY_SUB_QUEUE_URL" \
    --max-number-of-messages "$n" --wait-time-seconds 3 \
    --query 'Messages[].Body' --output text
}

ls_() {
  require_localstack
  echo "incoming:"  && aws_ s3 ls "s3://${BUCKET_INCOMING}/"  --recursive || true
  echo "validated:" && aws_ s3 ls "s3://${BUCKET_VALIDATED}/" --recursive || true
  echo "output:"    && aws_ s3 ls "s3://${BUCKET_OUTPUT}/"    --recursive || true
}

down() {
  require_localstack
  echo "Deleting demo resources ..."
  for b in "$BUCKET_INCOMING" "$BUCKET_VALIDATED" "$BUCKET_OUTPUT"; do
    aws_ s3 rb "s3://${b}" --force >/dev/null 2>&1 || true
  done
  aws_ sqs delete-queue --queue-url "$EVENTS_QUEUE_URL" >/dev/null 2>&1 || true
  aws_ sqs delete-queue --queue-url "$NOTIFY_SUB_QUEUE_URL" >/dev/null 2>&1 || true
  aws_ sns delete-topic --topic-arn "$NOTIFY_TOPIC_ARN" >/dev/null 2>&1 || true
  for p in temporal-address namespace task-queue api-key; do
    aws_ ssm delete-parameter --name "${SSM_PREFIX}/${p}" >/dev/null 2>&1 || true
  done
  echo "Done."
}

case "${1:-up}" in
  up)      up ;;
  trigger) shift; trigger "$@" ;;
  notify)  shift; notify "$@" ;;
  ls)      ls_ ;;
  down)    down ;;
  *) echo "Usage: $0 {up|trigger [S3URI]|notify [N]|ls|down}" >&2; exit 2 ;;
esac
