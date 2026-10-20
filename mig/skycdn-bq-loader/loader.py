#!/usr/bin/env python3
import concurrent.futures
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from google.api_core import exceptions as gexc
from google.cloud import bigquery, storage

PROJECT = os.environ["GCP_PROJECT"]
BQ_PROJECT = os.getenv("BQ_PROJECT", PROJECT)
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
STAGING_PREFIX = os.getenv("STAGING_PREFIX", "pending").strip("/")
MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "manifests").strip("/")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-west1")

# Manifest creation cadence. With the default of 120 seconds and at most one
# active manifest per destination table, the steady-state ceiling is <=720
# load jobs/table/day before retries, well below BigQuery's 1,500/day limit.
LOAD_INTERVAL_SECONDS = int(os.getenv("LOAD_INTERVAL_SECONDS", "120"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
MAX_URIS_PER_LOAD = int(os.getenv("MAX_URIS_PER_LOAD", "9000"))
MAX_CONCURRENT_LOADS = int(os.getenv("MAX_CONCURRENT_LOADS", "8"))
DELETE_WORKERS = int(os.getenv("DELETE_WORKERS", "32"))
DELETE_CHUNK_SIZE = int(os.getenv("DELETE_CHUNK_SIZE", "500"))
DELETE_RETRIES = int(os.getenv("DELETE_RETRIES", "6"))
DELETE_RETRY_BASE_SECONDS = float(os.getenv("DELETE_RETRY_BASE_SECONDS", "0.5"))
LOAD_RETRY_BASE_SECONDS = int(os.getenv("LOAD_RETRY_BASE_SECONDS", "30"))
LOAD_RETRY_MAX_SECONDS = int(os.getenv("LOAD_RETRY_MAX_SECONDS", "900"))
HEARTBEAT_SECONDS = int(os.getenv("HEARTBEAT_SECONDS", "60"))

storage_client = storage.Client(project=PROJECT)
bq_client = bigquery.Client(project=BQ_PROJECT)
bucket = storage_client.bucket(STAGING_BUCKET)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class ManifestDeferred(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc)


def iso_utc(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def log_event(event, **fields):
    payload = {"event": event, **fields}
    logging.info(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")))


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


def manifest_destination(name):
    prefix = f"{MANIFEST_PREFIX}/pending/"
    if not name.startswith(prefix):
        return None
    parts = name[len(prefix):].split("/")
    if len(parts) < 3:
        return None
    return parts[0], parts[1]


def create_manifest(dataset, table, names):
    batch_id = manifest_id(dataset, table, names)
    name = manifest_name(dataset, table, batch_id)
    blob = bucket.blob(name)
    now = utc_now()
    payload = json.dumps(
        {
            "batch_id": batch_id,
            "dataset": dataset,
            "table": table,
            "objects": names,
            "attempt": 0,
            "created_at": iso_utc(now),
        },
        separators=(",", ":"),
    ).encode()
    try:
        blob.upload_from_string(payload, content_type="application/json", if_generation_match=0)
        log_event(
            "manifest_created",
            manifest=name,
            dataset=dataset,
            table=table,
            files=len(names),
            batch_id=batch_id,
        )
    except gexc.PreconditionFailed:
        # Another creation of the same deterministic manifest already won.
        pass
    return name


def list_manifests():
    result = []
    prefix = f"{MANIFEST_PREFIX}/pending/"
    for blob in storage_client.list_blobs(STAGING_BUCKET, prefix=prefix):
        if blob.name.endswith(".json"):
            result.append(blob.name)
    return sorted(result)


def create_new_manifests(existing_manifests):
    """Create at most one new manifest per destination table.

    A destination with an existing manifest is skipped. This is the key v2
    invariant: tables progress independently, while source objects cannot be
    assigned to overlapping manifests for the same destination.
    """
    active_destinations = {
        destination
        for name in existing_manifests
        if (destination := manifest_destination(name)) is not None
    }

    groups = pending_groups()
    created = []
    for (dataset, table), names in sorted(groups.items()):
        if (dataset, table) in active_destinations:
            continue
        if not names:
            continue
        chunk = names[:MAX_URIS_PER_LOAD]
        created.append(create_manifest(dataset, table, chunk))
        active_destinations.add((dataset, table))
    return created


def load_job_id(data):
    base = f"skycdn_load_{data['dataset']}_{data['table']}_{data['batch_id']}"
    attempt = int(data.get("attempt", 0))
    return base if attempt == 0 else f"{base}_retry{attempt}"


def update_manifest_after_failed_job(blob, generation, data, error):
    attempt = int(data.get("attempt", 0)) + 1
    delay = min(LOAD_RETRY_MAX_SECONDS, LOAD_RETRY_BASE_SECONDS * (2 ** min(attempt - 1, 10)))
    retry_at = utc_now() + timedelta(seconds=delay)

    data = dict(data)
    data["attempt"] = attempt
    data["retry_not_before"] = iso_utc(retry_at)
    data["last_error"] = str(error)[:2000]
    data["last_failed_at"] = iso_utc(utc_now())

    blob.upload_from_string(
        json.dumps(data, separators=(",", ":")).encode(),
        content_type="application/json",
        if_generation_match=generation,
    )

    log_event(
        "load_retry_scheduled",
        manifest=blob.name,
        dataset=data["dataset"],
        table=data["table"],
        next_attempt=attempt,
        retry_after_seconds=delay,
    )


def delete_blob_with_retry(name):
    delay = DELETE_RETRY_BASE_SECONDS
    for attempt in range(1, DELETE_RETRIES + 1):
        try:
            bucket.blob(name).delete()
            return "deleted"
        except gexc.NotFound:
            return "missing"
        except (
            gexc.TooManyRequests,
            gexc.ServiceUnavailable,
            gexc.InternalServerError,
            gexc.BadGateway,
            gexc.GatewayTimeout,
            gexc.DeadlineExceeded,
        ) as exc:
            if attempt >= DELETE_RETRIES:
                raise
            log_event(
                "delete_retry",
                object=name,
                attempt=attempt,
                sleep_seconds=round(delay, 3),
                error=str(exc),
            )
            time.sleep(delay)
            delay = min(delay * 2, 10.0)


def delete_objects(objects, delete_executor):
    deleted = 0
    missing = 0

    # Avoid creating tens of thousands of Future objects at once on the small
    # loader VM. All load threads share the same bounded delete executor.
    for offset in range(0, len(objects), DELETE_CHUNK_SIZE):
        chunk = objects[offset:offset + DELETE_CHUNK_SIZE]
        futures = [delete_executor.submit(delete_blob_with_retry, obj) for obj in chunk]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result == "deleted":
                deleted += 1
            else:
                missing += 1

    return deleted, missing


def load_manifest(name, delete_executor):
    total_started = time.monotonic()

    blob = bucket.blob(name)
    blob.reload()
    generation = blob.generation
    data = json.loads(blob.download_as_text())

    batch_id = data["batch_id"]
    dataset = data["dataset"]
    table = data["table"]
    objects = data["objects"]
    attempt = int(data.get("attempt", 0))

    retry_not_before = parse_utc(data.get("retry_not_before"))
    if retry_not_before and utc_now() < retry_not_before:
        raise ManifestDeferred(name)

    job_id = load_job_id(data)
    table_id = f"{BQ_PROJECT}.{dataset}.{table}"
    uris = [f"gs://{STAGING_BUCKET}/{obj}" for obj in objects]

    queue_seconds = None
    created_at = parse_utc(data.get("created_at"))
    if created_at:
        queue_seconds = max(0.0, (utc_now() - created_at).total_seconds())

    try:
        job = bq_client.get_job(job_id, location=BQ_LOCATION)
        log_event(
            "load_job_reused",
            manifest=name,
            job_id=job_id,
            dataset=dataset,
            table=table,
            files=len(uris),
            attempt=attempt,
        )
    except gexc.NotFound:
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
        log_event(
            "load_job_started",
            manifest=name,
            job_id=job_id,
            dataset=dataset,
            table=table,
            files=len(uris),
            attempt=attempt,
            queue_seconds=round(queue_seconds, 3) if queue_seconds is not None else None,
        )

    load_started = time.monotonic()
    try:
        job.result()
    except Exception as exc:
        # Distinguish a terminal BigQuery job failure from a transient client/API
        # error. A failed load job is atomic, so it is safe to retry with a new
        # deterministic attempt suffix while keeping the same manifest.
        try:
            refreshed = bq_client.get_job(job_id, location=BQ_LOCATION)
        except Exception:
            raise

        if refreshed.state == "DONE" and refreshed.errors:
            update_manifest_after_failed_job(blob, generation, data, refreshed.errors)
        raise RuntimeError(f"BigQuery load job {job_id} failed: {exc}") from exc

    if job.errors:
        update_manifest_after_failed_job(blob, generation, data, job.errors)
        raise RuntimeError(f"BigQuery load job {job_id} failed: {job.errors}")

    load_seconds = time.monotonic() - load_started

    delete_started = time.monotonic()
    deleted, missing = delete_objects(objects, delete_executor)
    delete_seconds = time.monotonic() - delete_started

    # Only remove the manifest after every staging object has been deleted or
    # was already absent. If a delete fails, the manifest remains and the next
    # attempt reuses the successful BigQuery job instead of appending twice.
    try:
        blob.delete(if_generation_match=generation)
    except gexc.NotFound:
        pass

    total_seconds = time.monotonic() - total_started
    log_event(
        "manifest_completed",
        manifest=name,
        batch_id=batch_id,
        job_id=job_id,
        dataset=dataset,
        table=table,
        files=len(objects),
        deleted=deleted,
        missing=missing,
        output_rows=getattr(job, "output_rows", None),
        queue_seconds=round(queue_seconds, 3) if queue_seconds is not None else None,
        load_seconds=round(load_seconds, 3),
        delete_seconds=round(delete_seconds, 3),
        total_seconds=round(total_seconds, 3),
    )


def reap_completed(inflight):
    completed = []
    for future, info in list(inflight.items()):
        if not future.done():
            continue
        completed.append(future)
        try:
            future.result()
        except ManifestDeferred:
            pass
        except Exception:
            logging.exception(
                "Failed manifest gs://%s/%s destination=%s.%s",
                STAGING_BUCKET,
                info["name"],
                info["dataset"],
                info["table"],
            )
    for future in completed:
        inflight.pop(future, None)


def schedule_manifests(load_executor, delete_executor, inflight, manifest_names):
    if len(inflight) >= MAX_CONCURRENT_LOADS:
        return 0

    inflight_names = {info["name"] for info in inflight.values()}
    inflight_destinations = {(info["dataset"], info["table"]) for info in inflight.values()}
    scheduled = 0

    for name in manifest_names:
        if len(inflight) >= MAX_CONCURRENT_LOADS:
            break
        if name in inflight_names:
            continue
        destination = manifest_destination(name)
        if destination is None or destination in inflight_destinations:
            continue

        dataset, table = destination
        future = load_executor.submit(load_manifest, name, delete_executor)
        inflight[future] = {"name": name, "dataset": dataset, "table": table}
        inflight_names.add(name)
        inflight_destinations.add(destination)
        scheduled += 1

        log_event(
            "manifest_scheduled",
            manifest=name,
            dataset=dataset,
            table=table,
            inflight=len(inflight),
        )

    return scheduled


def main():
    if MAX_URIS_PER_LOAD > 10000:
        raise ValueError("MAX_URIS_PER_LOAD cannot exceed BigQuery's 10,000 URI/job limit")
    if MAX_CONCURRENT_LOADS < 1 or DELETE_WORKERS < 1:
        raise ValueError("MAX_CONCURRENT_LOADS and DELETE_WORKERS must be >= 1")
    if LOAD_INTERVAL_SECONDS < 1 or POLL_INTERVAL_SECONDS < 1:
        raise ValueError("LOAD_INTERVAL_SECONDS and POLL_INTERVAL_SECONDS must be >= 1")

    log_event(
        "loader_started",
        project=PROJECT,
        bq_project=BQ_PROJECT,
        bucket=STAGING_BUCKET,
        staging_prefix=STAGING_PREFIX,
        manifest_prefix=MANIFEST_PREFIX,
        load_interval_seconds=LOAD_INTERVAL_SECONDS,
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        max_uris_per_load=MAX_URIS_PER_LOAD,
        max_concurrent_loads=MAX_CONCURRENT_LOADS,
        delete_workers=DELETE_WORKERS,
        delete_chunk_size=DELETE_CHUNK_SIZE,
    )

    inflight = {}
    next_manifest_scan = 0.0
    next_heartbeat = 0.0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_CONCURRENT_LOADS,
        thread_name_prefix="bq-load",
    ) as load_executor, concurrent.futures.ThreadPoolExecutor(
        max_workers=DELETE_WORKERS,
        thread_name_prefix="gcs-delete",
    ) as delete_executor:
        while True:
            loop_started = time.monotonic()
            reap_completed(inflight)

            manifests = list_manifests()

            if loop_started >= next_manifest_scan:
                created = create_new_manifests(manifests)
                if created:
                    manifests = list_manifests()
                next_manifest_scan = loop_started + LOAD_INTERVAL_SECONDS

            scheduled = schedule_manifests(
                load_executor,
                delete_executor,
                inflight,
                manifests,
            )

            if loop_started >= next_heartbeat:
                log_event(
                    "loader_heartbeat",
                    pending_manifests=len(manifests),
                    inflight_loads=len(inflight),
                    scheduled=scheduled,
                )
                next_heartbeat = loop_started + HEARTBEAT_SECONDS

            elapsed = time.monotonic() - loop_started
            time.sleep(max(0.1, POLL_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
