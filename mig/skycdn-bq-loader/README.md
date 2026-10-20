# SkyCDN BigQuery file loader v2

Single-VM loader for the MIG file-load architecture.

The MIG workers write deterministic NDJSON objects under:

`gs://$STAGING_BUCKET/pending/<dataset>/<table>/...`

The v2 loader keeps the same idempotency boundary as v1 but removes the global serialization bottleneck.

## Flow

For every BigQuery destination table, the loader:

1. groups unclaimed staged NDJSON objects by destination;
2. creates at most one active deterministic manifest for that destination;
3. schedules manifests for different destination tables concurrently;
4. starts or reuses the deterministic BigQuery load job for the manifest;
5. waits for that load job independently of the other tables;
6. deletes staged NDJSON objects concurrently after a successful load;
7. deletes the manifest only after all staged objects have been deleted or were already absent.

A restart after a successful BigQuery load reuses the same job ID and performs cleanup only, preventing the same manifest from being appended twice.

If a BigQuery load job reaches `DONE` with errors, the manifest is retained and its persisted `attempt` is incremented. The next retry uses a deterministic `_retryN` suffix and exponential backoff. BigQuery load jobs are atomic, so a failed load attempt does not partially append the manifest.

## Why v2

The v1 loader had three sources of head-of-line blocking:

- it created new manifests only when **no** pending manifest existed anywhere;
- it processed every manifest serially;
- it deleted staged objects serially after each successful load.

That meant a slow or large destination could delay unrelated tables and allowed cleanup time to extend the queue before the next load could even be created.

v2 removes the global barrier while deliberately keeping **one active manifest per destination table**. This prevents overlapping manifests for the same staged files and keeps load-job creation bounded per destination.

## Defaults

- `LOAD_INTERVAL_SECONDS=120`
- `POLL_INTERVAL_SECONDS=10`
- `MAX_URIS_PER_LOAD=9000`
- `MAX_CONCURRENT_LOADS=8`
- `DELETE_WORKERS=32`
- `DELETE_CHUNK_SIZE=500`
- `DELETE_RETRIES=6`
- `DELETE_RETRY_BASE_SECONDS=0.5`
- `LOAD_RETRY_BASE_SECONDS=30`
- `LOAD_RETRY_MAX_SECONDS=900`
- `HEARTBEAT_SECONDS=60`
- `BQ_LOCATION=europe-west1`

## Structured log events

The loader emits JSON payloads inside Cloud Logging messages for the important lifecycle events:

- `loader_started`
- `loader_heartbeat`
- `manifest_created`
- `manifest_scheduled`
- `load_job_started`
- `load_job_reused`
- `load_retry_scheduled`
- `delete_retry`
- `manifest_completed`

`manifest_completed` contains `queue_seconds`, `load_seconds`, `delete_seconds`, `total_seconds`, `files`, and `output_rows`. This makes the loader contribution to end-to-end latency directly measurable.

## Initial POC settings

Start with the defaults on the existing `e2-small` loader VM. The loader spends most of its time waiting on BigQuery and Cloud Storage API calls, so increasing VM size should not be the first tuning lever.

The first validation should compare:

- staging object growth;
- manifest queue depth;
- `queue_seconds` p50/p95;
- BigQuery `load_seconds` p50/p95;
- delete duration;
- Pub/Sub backlog and oldest unacked age;
- end-to-end `GCS ready -> BQ visible` latency.

If concurrency needs tuning, change `MAX_CONCURRENT_LOADS` first.
