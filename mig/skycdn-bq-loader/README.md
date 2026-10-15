# SkyCDN BigQuery file loader

Single-VM loader for the MIG file-load architecture.

The MIG workers write deterministic NDJSON objects under:

`gs://$STAGING_BUCKET/pending/<dataset>/<table>/...`

The loader:

1. groups pending objects by BigQuery destination;
2. persists a deterministic batch manifest;
3. launches a BigQuery load job with a deterministic job ID;
4. waits for completion;
5. deletes staged NDJSON objects and the manifest only after success.

A restart after a successful load reuses the same BigQuery job ID, preventing the same manifest from being appended twice.

Defaults:

- `LOAD_INTERVAL_SECONDS=120`
- `MAX_URIS_PER_LOAD=5000`
- `BQ_LOCATION=europe-west1`
