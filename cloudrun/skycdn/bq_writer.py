import datetime as dt
import logging
import threading
from dataclasses import dataclass

from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

LOG = logging.getLogger(__name__)

PROTO_STRING_TYPES = {
    "STRING", "DATETIME", "TIME", "NUMERIC", "BIGNUMERIC", "GEOGRAPHY", "JSON",
}


def _proto_type(field_type: str) -> int:
    t = field_type.upper()
    if t in PROTO_STRING_TYPES:
        return descriptor_pb2.FieldDescriptorProto.TYPE_STRING
    if t in {"INTEGER", "INT64"}:
        return descriptor_pb2.FieldDescriptorProto.TYPE_INT64
    if t in {"FLOAT", "FLOAT64"}:
        return descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE
    if t in {"BOOLEAN", "BOOL"}:
        return descriptor_pb2.FieldDescriptorProto.TYPE_BOOL
    if t == "BYTES":
        return descriptor_pb2.FieldDescriptorProto.TYPE_BYTES
    if t == "DATE":
        return descriptor_pb2.FieldDescriptorProto.TYPE_INT32
    if t == "TIMESTAMP":
        return descriptor_pb2.FieldDescriptorProto.TYPE_INT64
    raise ValueError(f"Unsupported BigQuery type for this POC: {field_type}")


def _convert_value(field_type: str, value):
    if value is None:
        return None

    t = field_type.upper()
    if t in PROTO_STRING_TYPES:
        return str(value)
    if t in {"INTEGER", "INT64"}:
        return int(value)
    if t in {"FLOAT", "FLOAT64"}:
        return float(value)
    if t in {"BOOLEAN", "BOOL"}:
        return bool(value)
    if t == "BYTES":
        return value if isinstance(value, bytes) else str(value).encode("utf-8")
    if t == "DATE":
        date_value = value if isinstance(value, dt.date) else dt.date.fromisoformat(str(value))
        return (date_value - dt.date(1970, 1, 1)).days
    if t == "TIMESTAMP":
        if isinstance(value, dt.datetime):
            timestamp = value
        else:
            timestamp = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        epoch = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        return int((timestamp.astimezone(dt.timezone.utc) - epoch).total_seconds() * 1_000_000)
    raise ValueError(f"Unsupported BigQuery type for this POC: {field_type}")


def _build_dynamic_message(schema: list[bigquery.SchemaField], message_name: str):
    descriptor = descriptor_pb2.DescriptorProto(name=message_name)

    for number, schema_field in enumerate(schema, start=1):
        if schema_field.mode == "REPEATED":
            raise ValueError(f"REPEATED field not supported in this POC: {schema_field.name}")
        if schema_field.field_type.upper() in {"RECORD", "STRUCT"}:
            raise ValueError(f"STRUCT field not supported in this POC: {schema_field.name}")

        field = descriptor.field.add()
        field.name = schema_field.name
        field.number = number
        field.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        field.type = _proto_type(schema_field.field_type)

    file_descriptor = descriptor_pb2.FileDescriptorProto()
    file_descriptor.name = f"{message_name.lower()}.proto"
    file_descriptor.package = "clt"
    file_descriptor.syntax = "proto2"
    file_descriptor.message_type.add().CopyFrom(descriptor)

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    message_descriptor = pool.FindMessageTypeByName(f"clt.{message_name}")

    if hasattr(message_factory, "GetMessageClass"):
        message_class = message_factory.GetMessageClass(message_descriptor)
    else:
        message_class = message_factory.MessageFactory(pool).GetPrototype(message_descriptor)

    return descriptor, message_class


@dataclass
class PendingAppend:
    future: object
    rows: int


@dataclass
class TableWriter:
    project: str
    dataset: str
    table: str
    bq_client: bigquery.Client
    write_client: bigquery_storage_v1.BigQueryWriteClient
    target_request_bytes: int = 2_000_000

    def __post_init__(self):
        table_ref = f"{self.project}.{self.dataset}.{self.table}"
        table_obj = self.bq_client.get_table(table_ref)
        self.schema = list(table_obj.schema)
        self.proto_descriptor, self.message_class = _build_dynamic_message(
            self.schema,
            "BQRow",
        )

        parent = self.write_client.table_path(self.project, self.dataset, self.table)
        self.stream_name = f"{parent}/streams/_default"

        request_template = types.AppendRowsRequest()
        request_template.write_stream = self.stream_name

        proto_schema = types.ProtoSchema()
        proto_schema.proto_descriptor = self.proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        request_template.proto_rows = proto_data

        self.append_stream = writer.AppendRowsStream(self.write_client, request_template)
        LOG.info("Opened BigQuery default stream writer for %s", table_ref)

    def _serialize_row(self, row: dict) -> bytes:
        message = self.message_class()

        for field in self.schema:
            value = row.get(field.name)
            if value is None:
                continue
            converted = _convert_value(field.field_type, value)
            if converted is not None:
                setattr(message, field.name, converted)

        return message.SerializeToString()

    def _send_proto_rows(self, serialized_rows: list[bytes]):
        proto_rows = types.ProtoRows()
        proto_rows.serialized_rows.extend(serialized_rows)

        request = types.AppendRowsRequest()
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows
        request.proto_rows = proto_data
        return self.append_stream.send(request)

    def submit_rows(self, rows: list[dict]) -> list[PendingAppend]:
        """
        Serialize and enqueue AppendRows requests without waiting for their
        responses. The caller must eventually call future.result() on every
        returned PendingAppend before acknowledging the upstream message.
        """
        if not rows:
            return []

        pending: list[PendingAppend] = []
        batch: list[bytes] = []
        batch_bytes = 0

        for row in rows:
            serialized = self._serialize_row(row)
            row_bytes = len(serialized)

            if batch and batch_bytes + row_bytes > self.target_request_bytes:
                pending.append(PendingAppend(self._send_proto_rows(batch), len(batch)))
                batch = []
                batch_bytes = 0

            batch.append(serialized)
            batch_bytes += row_bytes

        if batch:
            pending.append(PendingAppend(self._send_proto_rows(batch), len(batch)))

        return pending

    def append_rows(self, rows: list[dict]) -> int:
        """Synchronous compatibility wrapper."""
        pending = self.submit_rows(rows)
        written = 0
        for append in pending:
            append.future.result()
            written += append.rows
        return written

    def close(self):
        self.append_stream.close()


class WriterManager:
    def __init__(self, project: str, target_request_bytes: int = 2_000_000):
        self.project = project
        self.target_request_bytes = target_request_bytes
        self.bq_client = bigquery.Client(project=project)
        self.write_client = bigquery_storage_v1.BigQueryWriteClient()
        self._writers: dict[tuple[str, str], TableWriter] = {}
        self._lock = threading.Lock()

    def get_writer(self, dataset: str, table: str) -> TableWriter:
        key = (dataset, table)
        writer_obj = self._writers.get(key)
        if writer_obj is not None:
            return writer_obj

        with self._lock:
            writer_obj = self._writers.get(key)
            if writer_obj is None:
                writer_obj = TableWriter(
                    project=self.project,
                    dataset=dataset,
                    table=table,
                    bq_client=self.bq_client,
                    write_client=self.write_client,
                    target_request_bytes=self.target_request_bytes,
                )
                self._writers[key] = writer_obj
            return writer_obj
