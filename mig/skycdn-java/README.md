# SkyCDN MIG Java POC — BigQuery file-load sink

Standalone Java worker for SkyCDN ingestion using a Pub/Sub pull subscription on a Compute Engine managed instance group.

Flow:

GCS OBJECT_FINALIZE -> Pub/Sub -> MIG Java worker -> deterministic NDJSON staging on GCS -> single loader VM -> BigQuery load jobs

The worker:
- uses StreamingPull with flow control;
- processes several source objects concurrently per VM;
- downloads raw GCS bytes and decompresses gzip locally;
- parses and routes SkyCDN rows with the existing routing semantics;
- writes newline-delimited JSON files to a staging bucket;
- uses deterministic staging object names derived from source bucket/object/generation and destination;
- treats an already-existing staging object as success, making Pub/Sub redelivery idempotent at the staging boundary;
- ACKs Pub/Sub only after every destination file for the source object has been staged successfully.

Default runtime settings:
- `WORKER_THREADS=4`
- `MAX_OUTSTANDING_MESSAGES=4`
- `MAX_OUTSTANDING_BYTES=536870912`
- `STAGING_PREFIX=pending`

The previous Storage Write API path has been removed from this branch to avoid the regional AppendRows throughput ceiling observed in the autoscaling test.

The separate `mig/skycdn-bq-loader` process batches staged files and submits append-mode BigQuery load jobs.
