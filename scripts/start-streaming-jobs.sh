#!/usr/bin/env bash
set -euo pipefail

# Launches one streaming Dataflow job per CDN/model from the same classic template.
#
# Required:
#   PROJECT
#   VERSION
#
# Optional:
#   REGION       default: europe-west1
#   MAX_WORKERS  default: 20
#   NUM_WORKERS  default: 1

: "${PROJECT:?PROJECT is required}"
: "${VERSION:?VERSION is required}"

REGION="${REGION:-europe-west1}"
MAX_WORKERS="${MAX_WORKERS:-20}"
NUM_WORKERS="${NUM_WORKERS:-1}"

TEMPLATE="gs://${PROJECT}-dataflow/templateLocation/it.sky.technology.telemetry.clt.CLTIngestorUKStreaming.${VERSION}"
DLQ_TOPIC="projects/${PROJECT}/topics/clt-ingestor-streaming-dlq"

launch() {
  local model="$1"
  local subscription="$2"
  local job_name="clt-ingestor-${model}-streaming"

  echo "Launching ${job_name}"

  gcloud dataflow jobs run "${job_name}" \
    --project="${PROJECT}" \
    --region="${REGION}" \
    --gcs-location="${TEMPLATE}" \
    --parameters="inputSubscription=projects/${PROJECT}/subscriptions/${subscription},model=${model},dlqTopic=${DLQ_TOPIC}" \
    --enable-streaming-engine \
    --num-workers="${NUM_WORKERS}" \
    --max-workers="${MAX_WORKERS}"
}

launch akamai     clt-ingestor-akamai-streaming
launch cloudfront clt-ingestor-cloudfront-streaming
launch skycdn     clt-ingestor-skycdn-streaming
launch raiway     clt-ingestor-raiway-streaming
