# SkyCDN Cloud Run Java POC

Standalone Java ingestion service for benchmarking the same GCS -> Pub/Sub -> Cloud Run -> BigQuery Storage Write flow used by the Python POC, without Beam/Dataflow.

## Flow

GCS OBJECT_FINALIZE -> Pub/Sub push -> Cloud Run Java -> BigQuery Storage Write API default stream

The first benchmark should use a manual HTTP POST against a separate Cloud Run service so the existing SkyCDN subscription remains untouched.

## Runtime defaults

- expected object prefix: `skycdn/`
- row buffer: 5,000 rows
- maximum in-flight BigQuery appends: 16
- HTTP worker threads: 4
- BigQuery default stream via `JsonStreamWriter`
- connection pooling enabled

## Environment variables

- `BQ_PROJECT` (required)
- `EXPECTED_PREFIX` (default `skycdn/`)
- `BQ_ROW_BUFFER_ROWS` (default `5000`)
- `BQ_MAX_PENDING_APPENDS` (default `16`)
- `HTTP_THREADS` (default `4`)

## Deploy benchmark service

```bash
export PROJECT=sky-it-telemetry-clt-stage
export REGION=europe-west1
export SERVICE_JAVA=clt-skycdn-ingestor-java
export RUNTIME_SA=clt-skycdn-ingestor@${PROJECT}.iam.gserviceaccount.com

gcloud run deploy "$SERVICE_JAVA" \
  --source=. \
  --project="$PROJECT" \
  --region="$REGION" \
  --service-account="$RUNTIME_SA" \
  --cpu=1 \
  --memory=1Gi \
  --concurrency=1 \
  --min=0 \
  --max=2 \
  --timeout=600 \
  --set-env-vars="BQ_PROJECT=${PROJECT},EXPECTED_PREFIX=skycdn/,BQ_ROW_BUFFER_ROWS=5000,BQ_MAX_PENDING_APPENDS=16,HTTP_THREADS=4" \
  --no-allow-unauthenticated
```

Get the URL:

```bash
export SERVICE_JAVA_URL=$(gcloud run services describe "$SERVICE_JAVA" \
  --project="$PROJECT" \
  --region="$REGION" \
  --format='value(status.url)')
```

Grant your own identity `roles/run.invoker` for manual testing if needed.

## Manual benchmark

Use the same object that was used for the Python benchmark.

```json
{
  "message": {
    "attributes": {
      "eventType": "OBJECT_FINALIZE",
      "bucketId": "sky-it-telemetry-clt-stage-ready",
      "objectId": "skycdn/replay/run_id=.../FILE.gz"
    },
    "messageId": "manual-java-test-1"
  },
  "subscription": "manual-test"
}
```

Call the service:

```bash
curl -i \
  -X POST \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  --data-binary @/tmp/test-message.json \
  "${SERVICE_JAVA_URL}/"
```

Then inspect logs:

```bash
gcloud run services logs read "$SERVICE_JAVA" \
  --project="$PROJECT" \
  --region="$REGION" \
  --limit=100
```

The final JSON log contains download, parse/write loop, BigQuery submit/wait and end-to-end timings.

## If Java wins the benchmark

Only after the manual benchmark should the existing subscription be pointed at the Java service:

```bash
export SUBSCRIPTION=clt-ingestor-skycdn-streaming
export PUSH_SA=clt-skycdn-pubsub-push@${PROJECT}.iam.gserviceaccount.com

gcloud run services add-iam-policy-binding "$SERVICE_JAVA" \
  --project="$PROJECT" \
  --region="$REGION" \
  --member="serviceAccount:${PUSH_SA}" \
  --role=roles/run.invoker

gcloud pubsub subscriptions modify-push-config "$SUBSCRIPTION" \
  --project="$PROJECT" \
  --push-endpoint="${SERVICE_JAVA_URL}/" \
  --push-auth-service-account="$PUSH_SA" \
  --push-auth-token-audience="$SERVICE_JAVA_URL"
```

Start with a low Cloud Run maximum instance count and increase progressively.

Rollback to pull mode:

```bash
gcloud pubsub subscriptions modify-push-config "$SUBSCRIPTION" \
  --project="$PROJECT" \
  --clear-push-config
```

## Semantics

The default BigQuery stream is at-least-once. Pub/Sub is acknowledged only after all appends for an object complete. A retry after a partial successful write can therefore duplicate rows. Object-generation idempotency should be added before production cutover.
