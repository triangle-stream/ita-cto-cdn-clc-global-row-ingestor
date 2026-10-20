#!/bin/bash
set -euxo pipefail

META="http://metadata.google.internal/computeMetadata/v1/instance/attributes"
TOKEN_URL="http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
HDR="Metadata-Flavor: Google"
DOCKER_CONFIG_DIR="/var/lib/skycdn-loader-docker-config"

meta() {
  local key="$1"
  curl -fsS -H "$HDR" "$META/$key"
}

IMAGE="$(meta IMAGE)"
PROJECT="$(meta PROJECT)"
BQ_PROJECT="$(meta BQ_PROJECT || true)"
STAGING_BUCKET="$(meta STAGING_BUCKET)"
STAGING_PREFIX="$(meta STAGING_PREFIX || true)"
BQ_LOCATION="$(meta BQ_LOCATION || true)"
LOAD_INTERVAL_SECONDS="$(meta LOAD_INTERVAL_SECONDS || true)"
POLL_INTERVAL_SECONDS="$(meta POLL_INTERVAL_SECONDS || true)"
MAX_URIS_PER_LOAD="$(meta MAX_URIS_PER_LOAD || true)"
MAX_CONCURRENT_LOADS="$(meta MAX_CONCURRENT_LOADS || true)"
DELETE_WORKERS="$(meta DELETE_WORKERS || true)"
DELETE_CHUNK_SIZE="$(meta DELETE_CHUNK_SIZE || true)"
DELETE_RETRIES="$(meta DELETE_RETRIES || true)"
DELETE_RETRY_BASE_SECONDS="$(meta DELETE_RETRY_BASE_SECONDS || true)"
LOAD_RETRY_BASE_SECONDS="$(meta LOAD_RETRY_BASE_SECONDS || true)"
LOAD_RETRY_MAX_SECONDS="$(meta LOAD_RETRY_MAX_SECONDS || true)"
HEARTBEAT_SECONDS="$(meta HEARTBEAT_SECONDS || true)"

BQ_PROJECT="${BQ_PROJECT:-$PROJECT}"
STAGING_PREFIX="${STAGING_PREFIX:-pending}"
BQ_LOCATION="${BQ_LOCATION:-europe-west1}"
LOAD_INTERVAL_SECONDS="${LOAD_INTERVAL_SECONDS:-120}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-10}"
MAX_URIS_PER_LOAD="${MAX_URIS_PER_LOAD:-9000}"
MAX_CONCURRENT_LOADS="${MAX_CONCURRENT_LOADS:-8}"
DELETE_WORKERS="${DELETE_WORKERS:-32}"
DELETE_CHUNK_SIZE="${DELETE_CHUNK_SIZE:-500}"
DELETE_RETRIES="${DELETE_RETRIES:-6}"
DELETE_RETRY_BASE_SECONDS="${DELETE_RETRY_BASE_SECONDS:-0.5}"
LOAD_RETRY_BASE_SECONDS="${LOAD_RETRY_BASE_SECONDS:-30}"
LOAD_RETRY_MAX_SECONDS="${LOAD_RETRY_MAX_SECONDS:-900}"
HEARTBEAT_SECONDS="${HEARTBEAT_SECONDS:-60}"

for i in $(seq 1 60); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

mkdir -p "$DOCKER_CONFIG_DIR"
chmod 700 "$DOCKER_CONFIG_DIR"

TOKEN_JSON="$(curl -fsS -H "$HDR" "$TOKEN_URL")"
ACCESS_TOKEN="$(echo "$TOKEN_JSON" | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
test -n "$ACCESS_TOKEN"

echo "$ACCESS_TOKEN" | docker --config "$DOCKER_CONFIG_DIR" login \
  -u oauth2accesstoken \
  --password-stdin \
  https://europe-west1-docker.pkg.dev

docker --config "$DOCKER_CONFIG_DIR" pull "$IMAGE"
docker rm -f skycdn-bq-loader || true

docker run -d \
  --name skycdn-bq-loader \
  --restart=always \
  -e GCP_PROJECT="$PROJECT" \
  -e BQ_PROJECT="$BQ_PROJECT" \
  -e STAGING_BUCKET="$STAGING_BUCKET" \
  -e STAGING_PREFIX="$STAGING_PREFIX" \
  -e BQ_LOCATION="$BQ_LOCATION" \
  -e LOAD_INTERVAL_SECONDS="$LOAD_INTERVAL_SECONDS" \
  -e POLL_INTERVAL_SECONDS="$POLL_INTERVAL_SECONDS" \
  -e MAX_URIS_PER_LOAD="$MAX_URIS_PER_LOAD" \
  -e MAX_CONCURRENT_LOADS="$MAX_CONCURRENT_LOADS" \
  -e DELETE_WORKERS="$DELETE_WORKERS" \
  -e DELETE_CHUNK_SIZE="$DELETE_CHUNK_SIZE" \
  -e DELETE_RETRIES="$DELETE_RETRIES" \
  -e DELETE_RETRY_BASE_SECONDS="$DELETE_RETRY_BASE_SECONDS" \
  -e LOAD_RETRY_BASE_SECONDS="$LOAD_RETRY_BASE_SECONDS" \
  -e LOAD_RETRY_MAX_SECONDS="$LOAD_RETRY_MAX_SECONDS" \
  -e HEARTBEAT_SECONDS="$HEARTBEAT_SECONDS" \
  "$IMAGE"
