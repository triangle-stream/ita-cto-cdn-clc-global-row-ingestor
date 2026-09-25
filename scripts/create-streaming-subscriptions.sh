#!/usr/bin/env bash
set -euo pipefail

# Creates one filtered subscription per CDN/model from an existing
# Cloud Storage Pub/Sub notification topic.
#
# Required:
#   PROJECT
#   TOPIC
#
# Optional:
#   ACK_DEADLINE (default 600)
#
# Example:
#   PROJECT=sky-it-telemetry-clt-stage \
#   TOPIC=ready-object-finalize \
#   ./scripts/create-streaming-subscriptions.sh
#
# IMPORTANT:
# Set the prefixes below to the actual object prefixes used in the ready bucket.
# The filter is evaluated against the Cloud Storage notification attribute objectId.

: "${PROJECT:?PROJECT is required}"
: "${TOPIC:?TOPIC is required}"

ACK_DEADLINE="${ACK_DEADLINE:-600}"

create_subscription() {
  local subscription="$1"
  local prefix="$2"

  if gcloud pubsub subscriptions describe "$subscription"       --project="$PROJECT" >/dev/null 2>&1; then
    echo "Subscription already exists: $subscription"
    return
  fi

  echo "Creating $subscription for object prefix $prefix"

  gcloud pubsub subscriptions create "$subscription"     --project="$PROJECT"     --topic="$TOPIC"     --ack-deadline="$ACK_DEADLINE"     --message-filter="attributes.eventType=\"OBJECT_FINALIZE\" AND hasPrefix(attributes.objectId, \"$prefix\")"
}

# Adjust these prefixes to match the ready bucket layout.
create_subscription "clt-ingestor-akamai-streaming"     "akamai/"
create_subscription "clt-ingestor-cloudfront-streaming" "cloudfront/"
create_subscription "clt-ingestor-skycdn-streaming"     "skycdn/"
create_subscription "clt-ingestor-raiway-streaming"     "raiway/"

# Application-level dead-letter topic. This receives service nomatches,
# malformed notifications, file read/parse errors and BigQuery rejected rows.
DLQ_TOPIC="clt-ingestor-streaming-dlq"
DLQ_SUBSCRIPTION="clt-ingestor-streaming-dlq-inspect"

if ! gcloud pubsub topics describe "$DLQ_TOPIC" --project="$PROJECT" >/dev/null 2>&1; then
  gcloud pubsub topics create "$DLQ_TOPIC" --project="$PROJECT"
fi

if ! gcloud pubsub subscriptions describe "$DLQ_SUBSCRIPTION"     --project="$PROJECT" >/dev/null 2>&1; then
  gcloud pubsub subscriptions create "$DLQ_SUBSCRIPTION"     --project="$PROJECT"     --topic="$DLQ_TOPIC"
fi

cat <<EOF

Created/verified streaming subscriptions.

Input subscription format expected by Dataflow:
  projects/$PROJECT/subscriptions/<subscription>

DLQ topic:
  projects/$PROJECT/topics/$DLQ_TOPIC

Do NOT run the new streaming jobs against the same BigQuery tables while the
legacy batch DAG is still consuming equivalent GCS notifications, unless
duplicate ingestion is explicitly acceptable.
EOF
