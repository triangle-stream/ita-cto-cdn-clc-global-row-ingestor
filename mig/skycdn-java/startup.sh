#!/bin/bash
set -euxo pipefail

IMAGE="${IMAGE:?missing IMAGE metadata/env}"
PROJECT="${PROJECT:?missing PROJECT metadata/env}"
SUBSCRIPTION="${SUBSCRIPTION:?missing SUBSCRIPTION metadata/env}"
BQ_PROJECT="${BQ_PROJECT:-$PROJECT}"
EXPECTED_PREFIX="${EXPECTED_PREFIX:-skycdn/}"
WORKER_THREADS="${WORKER_THREADS:-4}"
MAX_OUTSTANDING_MESSAGES="${MAX_OUTSTANDING_MESSAGES:-8}"
MAX_OUTSTANDING_BYTES="${MAX_OUTSTANDING_BYTES:-536870912}"
BQ_ROW_BUFFER_ROWS="${BQ_ROW_BUFFER_ROWS:-5000}"
BQ_MAX_PENDING_APPENDS="${BQ_MAX_PENDING_APPENDS:-16}"

for i in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then break; fi
  sleep 2
done

gcloud auth configure-docker europe-west1-docker.pkg.dev --quiet

docker pull "$IMAGE"
docker rm -f skycdn-worker || true

docker run -d \
  --name skycdn-worker \
  --restart=always \
  -e GCP_PROJECT="$PROJECT" \
  -e BQ_PROJECT="$BQ_PROJECT" \
  -e PUBSUB_SUBSCRIPTION="$SUBSCRIPTION" \
  -e EXPECTED_PREFIX="$EXPECTED_PREFIX" \
  -e WORKER_THREADS="$WORKER_THREADS" \
  -e MAX_OUTSTANDING_MESSAGES="$MAX_OUTSTANDING_MESSAGES" \
  -e MAX_OUTSTANDING_BYTES="$MAX_OUTSTANDING_BYTES" \
  -e BQ_ROW_BUFFER_ROWS="$BQ_ROW_BUFFER_ROWS" \
  -e BQ_MAX_PENDING_APPENDS="$BQ_MAX_PENDING_APPENDS" \
  "$IMAGE"
