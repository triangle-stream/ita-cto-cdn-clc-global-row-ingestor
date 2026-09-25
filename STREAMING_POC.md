# Streaming ingestor POC

This branch adds a streaming entrypoint without changing the current batch
entrypoint.

## Target topology

```
GCS ready bucket
     |
     | OBJECT_FINALIZE notification
     v
Pub/Sub topic
     |
     +-- filtered subscription: akamai/      -> streaming Dataflow --model=akamai
     +-- filtered subscription: cloudfront/  -> streaming Dataflow --model=cloudfront
     +-- filtered subscription: skycdn/      -> streaming Dataflow --model=skycdn
     +-- filtered subscription: raiway/      -> streaming Dataflow --model=raiway

Each job writes to the same dynamic BigQuery destinations used by the batch
pipeline. Application failures are published to a separate Pub/Sub DLQ topic.
```

The existing `CLTIngestorUK` batch class is intentionally unchanged.

## GCS layout

The cleanest layout is a single ready bucket with a prefix per CDN:

```
gs://<ready-bucket>/akamai/...
gs://<ready-bucket>/cloudfront/...
gs://<ready-bucket>/skycdn/...
gs://<ready-bucket>/raiway/...
```

Separate buckets are not required unless IAM, lifecycle or location policies
need to differ.

If the producer cannot be changed immediately, use subscription filters that
match the actual existing object names instead. The filter values in
`scripts/create-streaming-subscriptions.sh` are examples and must match the
real GCS object layout.

## Create subscriptions and application DLQ

Set the existing Cloud Storage notification topic and run:

```bash
PROJECT=<project> \
TOPIC=<existing-gcs-notification-topic> \
./scripts/create-streaming-subscriptions.sh
```

The script creates one subscription per CDN and one application DLQ topic plus
an inspection subscription.

New subscriptions only receive messages published after the subscription is
created. Creating the subscriptions is therefore safe while the legacy
subscription continues to be consumed, but starting both ingestion paths
against the same BigQuery destinations would duplicate data.

## Build the streaming classic template

```bash
gcloud builds submit \
  --config cloudbuild-streaming.yaml \
  --project=<project> \
  --substitutions _PROJECT=<project>,_REGION=europe-west1,_VERSION=0.1.0
```

## Start one streaming job

Example for Akamai:

```bash
gcloud dataflow jobs run clt-ingestor-akamai-streaming \
  --project=<project> \
  --region=europe-west1 \
  --gcs-location=gs://<project>-dataflow/templateLocation/it.sky.technology.telemetry.clt.CLTIngestorUKStreaming.0.1.0 \
  --parameters=inputSubscription=projects/<project>/subscriptions/clt-ingestor-akamai-streaming,model=akamai,dlqTopic=projects/<project>/topics/clt-ingestor-streaming-dlq \
  --enable-streaming-engine \
  --num-workers=1 \
  --max-workers=20
```

Repeat with the matching subscription and model for the other CDNs.

Start with a conservative maximum worker count and tune it after observing
Pub/Sub oldest-unacked-message age, Dataflow CPU/backlog and BigQuery write
throughput.

## What changed compared with the batch job

The model is supplied once per streaming job using `--model`. The parser loads
the model mapping and row mapper once in `@Setup`; it does not call
`detectModel(filename)` for every line.

Files are read with `BufferedReader` instead of `Scanner`.

The existing six global-count/TextIO nomatch branches are not used. The
streaming pipeline writes the following failures to the application Pub/Sub
DLQ:

- invalid or incomplete GCS notification;
- file read failure;
- service nomatch;
- line processing failure;
- BigQuery Storage Write API rejected row.

The current service lists, model JSON files, mapper factory and tests are
otherwise deliberately left unchanged for this POC.

## Cutover

Do not run the legacy DAG and the streaming pipelines simultaneously against
the same notification population and BigQuery destinations.

A controlled cutover is:

1. Create the filtered streaming subscriptions while the current DAG remains active.
2. Build the streaming template.
3. Stop scheduling the legacy DAG.
4. Record the old subscription backlog and decide whether to drain it with the
   legacy path or replay those GCS objects separately.
5. Start the streaming jobs.
6. Confirm that each new subscription's oldest unacked message age remains low
   and that rows appear in the expected BigQuery tables.
7. Only then retire the old subscription/DAG.

## Follow-up optimization

This POC intentionally keeps the existing `Map<String,String>` plus regex split
parser to limit behavioral changes. Once the streaming topology is stable,
profile worker CPU and GC. If parsing becomes material, the next step is a
model-specific parser that emits `TableRow` directly and avoids the temporary
map and generic regex tokenization.
