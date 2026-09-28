import base64
import datetime as dt
import gzip
import io
import json
import logging
import os
import time
from collections import defaultdict

from flask import Flask, jsonify, request
from google.cloud import storage

from bq_writer import WriterManager
from parser import destination_for, map_to_bq_row, parse_fields, resolve_service

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOG = logging.getLogger("skycdn-ingestor")

PROJECT_ID = os.environ.get("BQ_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    raise RuntimeError("BQ_PROJECT or GOOGLE_CLOUD_PROJECT must be set")

EXPECTED_PREFIX = os.getenv("EXPECTED_PREFIX", "CDN_ITA/skycdn/")
TARGET_REQUEST_BYTES = int(os.getenv("BQ_APPEND_TARGET_BYTES", "2000000"))
MAX_NOMATCH_LOG_SAMPLES = int(os.getenv("MAX_NOMATCH_LOG_SAMPLES", "3"))

storage_client = storage.Client()
writer_manager = WriterManager(
    project=PROJECT_ID,
    target_request_bytes=TARGET_REQUEST_BYTES,
)

app = Flask(__name__)


def _notification_from_envelope(envelope: dict) -> tuple[str, str, str | None, str | None]:
    message = envelope.get("message") or {}
    attributes = message.get("attributes") or {}

    bucket = attributes.get("bucketId")
    object_name = attributes.get("objectId")
    generation = attributes.get("objectGeneration")
    event_type = attributes.get("eventType")

    if (not bucket or not object_name) and message.get("data"):
        decoded = base64.b64decode(message["data"])
        payload = json.loads(decoded)
        bucket = bucket or payload.get("bucket")
        object_name = object_name or payload.get("name")
        generation = generation or payload.get("generation")

    if not bucket or not object_name:
        raise ValueError("Pub/Sub message does not contain bucket/object information")

    return bucket, object_name, generation, event_type


def _open_gzip_lines(bucket_name: str, object_name: str, generation: str | None):
    bucket = storage_client.bucket(bucket_name)
    generation_int = int(generation) if generation else None
    blob = bucket.blob(object_name, generation=generation_int)

    raw = blob.open("rb")
    gz = gzip.GzipFile(fileobj=raw, mode="rb")
    text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
    return text


def process_object(bucket_name: str, object_name: str, generation: str | None) -> dict:
    started = time.monotonic()
    date_insert = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    rows_by_destination: dict[tuple[str, str], list[dict]] = defaultdict(list)
    lines_read = 0
    parsed_rows = 0
    nomatch_rows = 0
    unsupported_rows = 0
    malformed_rows = 0
    nomatch_samples = []

    with _open_gzip_lines(bucket_name, object_name, generation) as lines:
        for raw_line in lines:
            line = raw_line.rstrip("\r\n")
            if not line:
                continue

            lines_read += 1
            try:
                parsed = parse_fields(line)
                service = resolve_service(parsed)

                if service == "nomatch":
                    nomatch_rows += 1
                    if len(nomatch_samples) < MAX_NOMATCH_LOG_SAMPLES:
                        nomatch_samples.append(line[:1000])
                    continue

                destination = destination_for(service)
                if destination is None:
                    unsupported_rows += 1
                    continue

                bq_row = map_to_bq_row(parsed, object_name, date_insert)
                rows_by_destination[destination].append(bq_row)
                parsed_rows += 1

            except Exception as exc:
                malformed_rows += 1
                LOG.warning(
                    "Skipping malformed SkyCDN row object=%s line=%d error=%s",
                    object_name,
                    lines_read,
                    exc,
                )

    written_rows = 0
    destination_counts = {}
    for (dataset, table), rows in rows_by_destination.items():
        table_writer = writer_manager.get_writer(dataset, table)
        written = table_writer.append_rows(rows)
        written_rows += written
        destination_counts[f"{dataset}.{table}"] = written

    elapsed = time.monotonic() - started
    result = {
        "bucket": bucket_name,
        "object": object_name,
        "generation": generation,
        "lines_read": lines_read,
        "parsed_rows": parsed_rows,
        "written_rows": written_rows,
        "nomatch_rows": nomatch_rows,
        "unsupported_rows": unsupported_rows,
        "malformed_rows": malformed_rows,
        "destinations": destination_counts,
        "elapsed_seconds": round(elapsed, 3),
    }

    if nomatch_samples:
        LOG.info("Nomatch samples object=%s samples=%s", object_name, nomatch_samples)
    LOG.info("Processed SkyCDN object %s", json.dumps(result, sort_keys=True))
    return result


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True}), 200


@app.post("/")
def pubsub_push():
    envelope = request.get_json(silent=True)
    if not isinstance(envelope, dict):
        return jsonify({"error": "invalid Pub/Sub envelope"}), 400

    try:
        bucket, object_name, generation, event_type = _notification_from_envelope(envelope)

        if event_type and event_type != "OBJECT_FINALIZE":
            return ("", 204)

        if EXPECTED_PREFIX and not object_name.startswith(EXPECTED_PREFIX):
            LOG.warning("Ignoring object outside expected prefix: %s", object_name)
            return ("", 204)

        process_object(bucket, object_name, generation)
        return ("", 204)

    except Exception:
        LOG.exception("SkyCDN ingestion failed")
        return jsonify({"error": "ingestion failed; retry requested"}), 500
