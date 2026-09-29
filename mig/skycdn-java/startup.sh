#!/bin/bash
set -euxo pipefail

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
TOKEN_URL="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
HDR="Metadata-Flavor: Google"
meta() {
  local key="$1"
  curl -fsS -H "$HDR" "$META/$key"
}

IMAGE="$(meta IMAGE)"
PROJECT="$(meta PROJECT)"
SUBSCRIPTION="$(meta SUBSCRIPTION)"
BQ_PROJECT="$(meta BQ_PROJECT || true)"
EXPECTED_PREFIX="$(meta EXPECTED_PREFIX || true)"
WORKER_THREADS="$(meta WORKER_THREADS || true)"
MAX_OUTSTANDING_MESSAGES="$(meta MAX_OUTSTANDING_MESSAGES || true)"
MAX_OUTSTANDING_BYTES="$(meta MAX_OUTSTANDING_BYTES || true)"
BQ_ROW_BUFFER_ROWS="$(meta BQ_ROW_BUFFER_ROWS || true)"
BQ_MAX_PENDING_APPENDS="$(meta BQ_MAX_PENDING_APPENDS || true)"

BQ_PROJECT="${BQ_PROJECT:-$PROJECT}"
EXPECTED_PREFIX="${EXPECTED_PREFIX:-skycdn/}"
WORKER_THREADS="${WORKER_THREADS:-4}"
MAX_OUTSTANDING_MESSAGES="${MAX_OUTSTANDING_MESSAGES:-8}"
MAX_OUTSTANDING_BYTES="${MAX_OUTSTANDING_BYTES:-536870912}"
BQ_ROW_BUFFER_ROWS="${BQ_ROW_BUFFER_ROWS:-5000}"
BQ_MAX_PENDING_APPENDS="${BQ_MAX_PENDING_APPENDS:-16}"

for i in $(seq 1 60); do
  if docker info >/dev/null 2>&1; then break; fi
  sleep 2
done

TOKEN_JSON="$(curl -fsS -H "$HDR" "$TOKEN_URL")"
ACCESS_TOKEN="$(echo "$TOKEN_JSON" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
test -n "$ACCESS_TOKEN"
echo "$ACCESS_TOKEN" | docker login -u oauth2accesstoken --password-stdin https://europe-west1-docker.pkg.dev

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
