# SkyCDN MIG Java POC

Standalone Java worker for SkyCDN ingestion using a Pub/Sub pull subscription on a Compute Engine managed instance group.

Flow:

GCS OBJECT_FINALIZE -> existing SkyCDN Pub/Sub subscription -> MIG VM Java worker -> BigQuery Storage Write API

The worker:
- uses StreamingPull with flow control;
- processes several objects concurrently per VM;
- downloads each object from GCS, decompresses gzip if needed, parses SkyCDN rows and routes them using the current service-ordering semantics;
- reuses persistent BigQuery JsonStreamWriter instances;
- ACKs a Pub/Sub message only after all BigQuery appends for the object have completed;
- NACKs on failure so Pub/Sub can redeliver.

Default runtime settings:
- `WORKER_THREADS=4`
- `MAX_OUTSTANDING_MESSAGES=8`
- `MAX_OUTSTANDING_BYTES=536870912`
- `BQ_ROW_BUFFER_ROWS=5000`
- `BQ_MAX_PENDING_APPENDS=16`

The POC should initially be run with a one-VM MIG and no autoscaler. Measure files/sec and rows/sec first, then configure backlog-based autoscaling.
