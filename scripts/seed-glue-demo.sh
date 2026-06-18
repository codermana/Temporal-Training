#!/usr/bin/env bash
# Seed (and drive) the Day-6 "Spring Boot orchestrates Glue" demo on LocalStack.
#
# It stands in for producer "A" and the AWS control plane: it creates the S3
# bucket (the lake), the SQS trigger queue (the bus), the SNS topic + a subscribed
# SQS queue (A's inbox), and lands a fake Parquet partition. Glue is a paid-tier
# emulator on LocalStack (and we never call real AWS), so the running Spring Boot
# app supervises a self-hosted local stitch job that merges the parts itself.
#
# Usage:
#   scripts/seed-glue-demo.sh up                 # create resources + land a partition
#   scripts/seed-glue-demo.sh trigger [PREFIX]   # drop a trigger message on the bus (A's signal)
#   scripts/seed-glue-demo.sh notify [N]         # read up to N messages from A's inbox
#   scripts/seed-glue-demo.sh ls                 # list raw + curated S3 objects
#   scripts/seed-glue-demo.sh down               # delete every demo resource
#
# Matches the defaults in
# examples/runnable/17-spring-glue-pipeline/java/src/main/resources/application.yml.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT:-http://127.0.0.1:4566}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT="000000000000"

BUCKET="${AWS_BUCKET:-lake}"
RAW_PREFIX="${AWS_RAW_PREFIX:-raw/orders/}"
DEFAULT_PARTITION="${RAW_PREFIX}dt=2026-06-17/"
TRIGGER_QUEUE="glue-triggers"
NOTIFY_TOPIC="lake-validated"
NOTIFY_SUB_QUEUE="lake-validated-sub"

TRIGGER_QUEUE_URL="${ENDPOINT}/${ACCOUNT}/${TRIGGER_QUEUE}"
NOTIFY_SUB_QUEUE_URL="${ENDPOINT}/${ACCOUNT}/${NOTIFY_SUB_QUEUE}"
NOTIFY_TOPIC_ARN="arn:aws:sns:${REGION}:${ACCOUNT}:${NOTIFY_TOPIC}"
NOTIFY_SUB_QUEUE_ARN="arn:aws:sqs:${REGION}:${ACCOUNT}:${NOTIFY_SUB_QUEUE}"

# Prefer awslocal if installed; otherwise plain aws with the LocalStack endpoint
# and throwaway credentials (LocalStack ignores the values, but the SDK requires
# them to be set).
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
  echo "Creating S3 bucket s3://${BUCKET} ..."
  aws_ s3api create-bucket --bucket "$BUCKET" >/dev/null 2>&1 || true

  echo "Creating SQS trigger queue ${TRIGGER_QUEUE} (the bus) ..."
  aws_ sqs create-queue --queue-name "$TRIGGER_QUEUE" >/dev/null

  echo "Creating SNS topic ${NOTIFY_TOPIC} + subscriber queue ${NOTIFY_SUB_QUEUE} (A's inbox) ..."
  aws_ sns create-topic --name "$NOTIFY_TOPIC" >/dev/null
  aws_ sqs create-queue --queue-name "$NOTIFY_SUB_QUEUE" >/dev/null
  # RawMessageDelivery=true so the SQS body IS the published JSON (no SNS envelope).
  aws_ sns subscribe \
    --topic-arn "$NOTIFY_TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$NOTIFY_SUB_QUEUE_ARN" \
    --attributes RawMessageDelivery=true \
    --return-subscription-arn >/dev/null

  land_partition "$DEFAULT_PARTITION"
  echo
  echo "Seeded. Trigger queue : ${TRIGGER_QUEUE_URL}"
  echo "        Notify topic  : ${NOTIFY_TOPIC_ARN}"
  echo "        A's inbox     : ${NOTIFY_SUB_QUEUE_URL}"
}

# Land a few fake Parquet "part" files under a partition prefix (producer A).
land_partition() {
  local prefix="$1"
  echo "Landing Parquet partition s3://${BUCKET}/${prefix} ..."
  local tmp
  tmp="$(mktemp)"
  for n in 0000 0001 0002; do
    printf 'PAR1 fake parquet part %s\n' "$n" >"$tmp"
    aws_ s3api put-object --bucket "$BUCKET" --key "${prefix}part-${n}.parquet" --body "$tmp" >/dev/null
  done
  rm -f "$tmp"
}

trigger() {
  require_localstack
  local prefix="${1:-$DEFAULT_PARTITION}"
  local body
  body="$(printf '{"bucket":"%s","prefix":"%s"}' "$BUCKET" "$prefix")"
  echo "Dropping trigger on the bus: ${body}"
  aws_ sqs send-message --queue-url "$TRIGGER_QUEUE_URL" --message-body "$body" \
    --query MessageId --output text
}

notify() {
  require_localstack
  local n="${1:-5}"
  echo "Reading up to ${n} messages from A's inbox (${NOTIFY_SUB_QUEUE}) ..."
  aws_ sqs receive-message --queue-url "$NOTIFY_SUB_QUEUE_URL" \
    --max-number-of-messages "$n" --wait-time-seconds 2 \
    --query 'Messages[].Body' --output text
}

ls_objects() {
  require_localstack
  echo "raw:"
  aws_ s3 ls "s3://${BUCKET}/${RAW_PREFIX}" --recursive || true
  echo "curated:"
  aws_ s3 ls "s3://${BUCKET}/curated/" --recursive || true
}

down() {
  require_localstack
  echo "Tearing down demo resources ..."
  aws_ s3 rb "s3://${BUCKET}" --force >/dev/null 2>&1 || true
  aws_ sqs delete-queue --queue-url "$TRIGGER_QUEUE_URL" >/dev/null 2>&1 || true
  aws_ sqs delete-queue --queue-url "$NOTIFY_SUB_QUEUE_URL" >/dev/null 2>&1 || true
  aws_ sns delete-topic --topic-arn "$NOTIFY_TOPIC_ARN" >/dev/null 2>&1 || true
  echo "Done."
}

case "${1:-up}" in
  up)      up ;;
  trigger) shift; trigger "${1:-}" ;;
  notify)  shift; notify "${1:-5}" ;;
  ls)      ls_objects ;;
  down)    down ;;
  *)
    echo "Usage: scripts/seed-glue-demo.sh {up|trigger [PREFIX]|notify [N]|ls|down}" >&2
    exit 2
    ;;
esac
