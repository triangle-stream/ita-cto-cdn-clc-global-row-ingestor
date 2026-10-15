#!/usr/bin/env python3
import hashlib
import json
import logging
import os
import time
from collections import defaultdict

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

PROJECT = os.environ["GCP_PROJECT"]
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
STAGING_PREFIX = os.getenv("STAGING_PREFIX", "pending").strip("/")
MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "manifests").strip("/")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-west1")
LOAD_INTERVAL_SECONDS = int(os.getenv("LOAD_INTERVAL_SECONDS", "120"))
MAX_URIS_PER_LOAD = int(os.getenv("MAX_URIS_PER_LOAD", "5000"))

storage_client = storage.Client(project=PROJECT)
bq_client = bigquery.Client(project=PROJECT)
bucket = storage_client.bucket(STAGING_BUCKET)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def pending_groups():
    groups = defaultdict(list)
    prefix = STAGING_PREFIX + "/"
    for blob in storage_client.list_blobs(STAGING_BUCKET, prefix=prefix):
        if not blob.name.endswith(".ndjson"):
            continue
        parts = blob.name.split("/")
        if len(parts) < 5:
            continue
        dataset, table = parts[1], parts[2]
        groups[(dataset, table)].append(blob.name)
    for names in groups.values():
        names.sort()
    return groups


def manifest_id(dataset, table, names):
    h = hashlib.sha256()
    h.update(dataset.encode())
    h.update(b"\0")
    h.update(table.encode())
    for name in names:
        h.update(b"\0")
        h.update(name.encode())
    return h.hexdigest()[:32]


def manifest_name(dataset, table, batch_id):
    return f"{MANIFEST_PREFIX}/pending/{dataset}/{table}/{batch_id}.json"


def create_manifest(dataset, table, names):
    batch_id = manifest_id(dataset, table, names)
    name = manifest_name(dataset, table, batch_id)
    blob = bucket.blob(name)
    payload = json.dumps(
        {"batch_id": batch_id, "dataset": dataset, "table": table, "objects": names},
        separators=(",", ":"),
    ).encode()
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
    except Exception:
        if not blob.exists():
            raise
    return name


def list_manifests():
    result = []
    prefix = f"{MANIFEST_PREFIX}/pending/"
    for blob in storage_client.list_blobs(STAGING_BUCKET, prefix=prefix):
        if blob.name.endswith(".json"):
            result.append(blob.name)
    return sorted(result)


def load_manifest(name):
    blob = bucket.blob(name)
    data = json.loads(blob.download_as_text())
    batch_id = data["batch_id"]
    dataset = data["dataset"]
    table = data["table"]
    objects = data["objects"]

    job_id = f"skycdn_load_{dataset}_{table}_{batch_id}"
    table_id = f"{PROJECT}.{dataset}.{table}"
    uris = [f"gs://{STAGING_BUCKET}/{obj}" for obj in objects]

    try:
        job = bq_client.get_job(job_id, location=BQ_LOCATION)
        logging.info("Reusing existing load job %s", job_id)
    except NotFound:
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq_client.load_table_from_uri(
            uris,
            table_id,
            job_id=job_id,
            location=BQ_LOCATION,
            job_config=config,
        )
        logging.info("Started load job %s table=%s files=%d", job_id, table_id, len(uris))

    job.result()

    if job.errors:
        raise RuntimeError(f"BigQuery load job failed: {job.errors}")

    deleted = 0
    for obj in objects:
        source = bucket.blob(obj)
        try:
            source.delete()
            deleted += 1
        except NotFound:
            pass

    bucket.blob(name).delete()
    logging.info(
        "Completed load job %s table=%s files=%d deleted=%d output_rows=%s",
        job_id, table_id, len(objects), deleted, getattr(job, "output_rows", None),
    )


def create_new_manifests():
    groups = pending_groups()
    created = 0
    for (dataset, table), names in groups.items():
        for i in range(0, len(names), MAX_URIS_PER_LOAD):
            chunk = names[i:i + MAX_URIS_PER_LOAD]
            create_manifest(dataset, table, chunk)
            created += 1
    return created


def run_once():
    manifests = list_manifests()
    if not manifests:
        created = create_new_manifests()
        if created:
            manifests = list_manifests()

    for name in manifests:
        try:
            load_manifest(name)
        except Exception:
            logging.exception("Failed manifest gs://%s/%s", STAGING_BUCKET, name)


def main():
    logging.info(
        "Starting SkyCDN BigQuery file loader project=%s bucket=%s interval=%ss maxUris=%d",
        PROJECT, STAGING_BUCKET, LOAD_INTERVAL_SECONDS, MAX_URIS_PER_LOAD,
    )
    while True:
        started = time.monotonic()
        run_once()
        elapsed = time.monotonic() - started
        time.sleep(max(1, LOAD_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
