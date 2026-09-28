# SkyCDN Cloud Run ingestion POC

This POC replaces the Dataflow batch/streaming path for SkyCDN with:

GCS ready -> Pub/Sub push -> Cloud Run -> BigQuery Storage Write API

The service reads one gzip object per Pub/Sub request, parses SkyCDN rows using the existing model/routing logic, groups rows by final destination table, and writes them using the BigQuery Storage Write API default stream.

## Runtime defaults

- expected object prefix: `CDN_ITA/skycdn/`
- one Cloud Run instance handles one request at a time (`concurrency=1` should be used at deploy time)
- target BigQuery append size: 2,000,000 bytes
- Pub/Sub is acknowledged only after all BigQuery appends for that object complete successfully

## Required environment variables

- `BQ_PROJECT`: project containing the destination BigQuery datasets/tables
- `EXPECTED_PREFIX`: defaults to `CDN_ITA/skycdn/`
- `BQ_APPEND_TARGET_BYTES`: defaults to `2000000`

## IAM required by the Cloud Run runtime service account

- `roles/storage.objectViewer` on the ready bucket
- `roles/bigquery.dataEditor` on destination datasets (or project for POC)
- `roles/bigquery.jobUser` is not required for Storage Write itself, but may already be present

The service dynamically reads the existing BigQuery table schema and builds the protobuf schema used by Storage Write API.

## Deploy

For the first test, deliberately cap scaling at two instances. Increase only after validating throughput and row correctness.

From this directory:

```bash
gcloud run deploy clt-skycdn-ingestor \
  --source=. \
  --project=<project> \
  --region=europe-west1 \
  --service-account=<runtime-sa> \
  --cpu=1 \
  --memory=1Gi \
  --concurrency=1 \
  --min=0 \
  --max=2 \
  --timeout=600 \
  --set-env-vars=BQ_PROJECT=<project>,EXPECTED_PREFIX=CDN_ITA/skycdn/,BQ_APPEND_TARGET_BYTES=2000000 \
  --no-allow-unauthenticated
```

Then retrieve the URL:

```bash
SERVICE_URL=$(gcloud run services describe clt-skycdn-ingestor \
  --project=<project> \
  --region=europe-west1 \
  --format='value(status.url)')
```

## Convert the existing SkyCDN subscription to push

Do not create another subscription if `clt-ingestor-skycdn-streaming` already exists.

Create a dedicated push identity:

```bash
gcloud iam service-accounts create clt-skycdn-pubsub-push \
  --project=<project>

PUSH_SA=clt-skycdn-pubsub-push@<project>.iam.gserviceaccount.com
```

Allow it to invoke the Cloud Run service:

```bash
gcloud run services add-iam-policy-binding clt-skycdn-ingestor \
  --project=<project> \
  --region=europe-west1 \
  --member="serviceAccount:${PUSH_SA}" \
  --role=roles/run.invoker
```

Allow the Pub/Sub service agent to mint OIDC tokens for the push service account:

```bash
PROJECT_NUMBER=$(gcloud projects describe <project> --format='value(projectNumber)')

gcloud projects add-iam-policy-binding <project> \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com" \
  --role=roles/iam.serviceAccountTokenCreator
```

Set the subscription acknowledgement deadline to the Pub/Sub maximum of 600 seconds before enabling push:

```bash
gcloud pubsub subscriptions update clt-ingestor-skycdn-streaming \
  --project=<project> \
  --ack-deadline=600
```

Update the existing subscription:

```bash
gcloud pubsub subscriptions modify-push-config clt-ingestor-skycdn-streaming \
  --project=<project> \
  --push-endpoint="${SERVICE_URL}/" \
  --push-auth-service-account="${PUSH_SA}" \
  --push-auth-token-audience="${SERVICE_URL}"
```

## First test

Before enabling push on a large existing backlog, call the Cloud Run endpoint manually with one known gzip object using a Pub/Sub-shaped JSON payload. Then enable push with `max-instances=2` and observe backlog/latency.

Watch logs:

```bash
gcloud run services logs read clt-skycdn-ingestor \
  --project=<project> \
  --region=europe-west1 \
  --limit=100
```

Useful monitoring metrics are `num_undelivered_messages` and `oldest_unacked_message_age` in Cloud Monitoring.

## Scale after validation

For example:

```bash
gcloud run services update clt-skycdn-ingestor \
  --project=<project> \
  --region=europe-west1 \
  --max=20
```

Increase gradually while observing Pub/Sub backlog, Cloud Run CPU, request latency, and BigQuery write errors.

## Rollback to pull mode

To stop push delivery without deleting the subscription:

```bash
gcloud pubsub subscriptions modify-push-config clt-ingestor-skycdn-streaming \
  --project=<project> \
  --clear-push-config
```

The backlog remains on the subscription.
