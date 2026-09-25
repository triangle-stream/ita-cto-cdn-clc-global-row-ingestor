package com.sky.ingestor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sky.ingestor.utils.RowMapper;
import com.sky.ingestor.utils.RowMapperFactory;

/**
 * Streaming variant of CLTIngestorUK.
 *
 * One instance is intended to consume one Pub/Sub subscription for one CDN/model.
 * The model is supplied once as a runtime option instead of being detected for every log line.
 *
 * This class intentionally leaves the existing batch entrypoint untouched.
 */
public class CLTIngestorUKStreaming {

    public interface CustomPipelineOptions extends DataflowPipelineOptions {
        @Description("Fully-qualified Pub/Sub subscription, e.g. projects/PROJECT/subscriptions/SUB")
        @Validation.Required
        ValueProvider<String> getInputSubscription();
        void setInputSubscription(ValueProvider<String> value);

        @Description("CDN model handled by this streaming job, e.g. akamai, skycdn, cloudfront, raiway")
        @Validation.Required
        ValueProvider<String> getModel();
        void setModel(ValueProvider<String> value);

        @Description("Fully-qualified Pub/Sub topic used as application DLQ")
        @Validation.Required
        ValueProvider<String> getDlqTopic();
        void setDlqTopic(ValueProvider<String> value);
    }

    private static final Logger LOG = LoggerFactory.getLogger(CLTIngestorUKStreaming.class);

    private static final DateTimeFormatter DT_INS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                         .withZone(ZoneOffset.UTC);

    // Keep service routing identical to the existing batch implementation for now.
    private static final Set<String> OTT_SERVICES = Set.of(
        "nowtv_linear", "nowtv_vod",
        "skygo_linear", "skygo_vod"
    );

    private static final Set<String> SOIP_SERVICES = Set.of(
        "soip_linear", "soip_vod", "soip_ads", "soip_npvr", "soip_ivod"
    );

    private static final Set<String> LEGACY_SERVICES = Set.of(
        "hip_linear", "hip_vod",
        "verdi_linear", "verdi_vod",
        "vod-stb"
    );

    private static final TupleTag<String> URI_TAG = new TupleTag<String>() {};
    private static final TupleTag<KV<String, TableRow>> BQ_ROWS_TAG =
        new TupleTag<KV<String, TableRow>>() {};
    private static final TupleTag<String> DLQ_TAG = new TupleTag<String>() {};

    private static String datasetFor(String model, String service) {
        final String group;
        if (LEGACY_SERVICES.contains(service)) {
            group = "legacy";
        } else if (SOIP_SERVICES.contains(service)) {
            group = "soip";
        } else {
            group = "ott";
        }
        return String.format("it_%s_%s_logs", model, group);
    }

    private static String tableFor(String service) {
        return service.replace('-', '_');
    }

    private static String tableSpec(String project, String dataset, String table) {
        return project + ":" + dataset + "." + table;
    }

    private static String objectNameFromUri(String gcsUri) {
        if (gcsUri != null && gcsUri.startsWith("gs://")) {
            int idx = gcsUri.indexOf('/', 5);
            if (idx > 0 && idx + 1 < gcsUri.length()) {
                return gcsUri.substring(idx + 1);
            }
        }
        return gcsUri;
    }

    private static String safeDlqJson(
        ObjectMapper mapper,
        String reason,
        String model,
        String fileName,
        String service,
        String rawLine,
        String detail) {

        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("reason", reason);
            payload.put("model", model);
            payload.put("file_name", fileName);
            payload.put("service", service);
            payload.put("raw_line", rawLine);
            payload.put("detail", detail);
            payload.put("processing_time", Instant.now().toString());
            return mapper.writeValueAsString(payload);
        } catch (Exception e) {
            return "{\"reason\":\"dlq_serialization_error\",\"detail\":\""
                + String.valueOf(e.getMessage()).replace("\\", "\\\\").replace("\"", "\\\"")
                + "\"}";
        }
    }

    /**
     * Converts a Cloud Storage Pub/Sub notification to a gs:// URI.
     * GCS notification attributes are preferred; the JSON payload is only a fallback
     * to remain compatible with the payload consumed by the existing Airflow DAG.
     */
    static class NotificationToUriFn extends DoFn<PubsubMessage, String> {
        private transient ObjectMapper json;

        @Setup
        public void setup() {
            json = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            PubsubMessage msg = c.element();
            String eventType = msg.getAttribute("eventType");

            if (eventType != null && !"OBJECT_FINALIZE".equals(eventType)) {
                return;
            }

            String bucket = msg.getAttribute("bucketId");
            String object = msg.getAttribute("objectId");

            if ((bucket == null || object == null) && msg.getPayload() != null) {
                try {
                    JsonNode root = json.readTree(msg.getPayload());
                    if (bucket == null && root.hasNonNull("bucket")) {
                        bucket = root.get("bucket").asText();
                    }
                    if (object == null && root.hasNonNull("name")) {
                        object = root.get("name").asText();
                    }
                } catch (Exception e) {
                    c.output(DLQ_TAG, safeDlqJson(
                        json,
                        "invalid_gcs_notification",
                        null,
                        null,
                        null,
                        null,
                        e.getMessage()));
                    return;
                }
            }

            if (bucket == null || bucket.isBlank() || object == null || object.isBlank()) {
                c.output(DLQ_TAG, safeDlqJson(
                    json,
                    "missing_gcs_object_reference",
                    null,
                    object,
                    null,
                    null,
                    "Missing bucketId/objectId attributes and no usable payload fallback"));
                return;
            }

            c.output(URI_TAG, "gs://" + bucket + "/" + object);
        }
    }

    /**
     * Reads and parses one gzip file at a time.
     *
     * The model, mapping and RowMapper are resolved in @Setup for the job instance.
     * Therefore model detection is no longer repeated for every log line.
     */
    static class ParseFileFn extends DoFn<FileIO.ReadableFile, KV<String, TableRow>> {
        private final ValueProvider<String> modelProvider;

        private transient ObjectMapper json;
        private transient Map<String, Integer> mapping;
        private transient RowMapper rowMapper;
        private transient String model;
        private transient String bundleInsertTs;

        ParseFileFn(ValueProvider<String> modelProvider) {
            this.modelProvider = modelProvider;
        }

        @Setup
        public void setup() throws Exception {
            json = new ObjectMapper();
            model = modelProvider.get();
            if (model == null || model.isBlank()) {
                throw new IllegalArgumentException("--model cannot be empty");
            }
            model = model.trim().toLowerCase();

            Map<String, Map<String, Integer>> models = ModelResolver.loadAllModels();
            mapping = models.get(model);
            if (mapping == null) {
                throw new IllegalArgumentException("No model mapping found for " + model);
            }

            rowMapper = RowMapperFactory.get(model);
        }

        @StartBundle
        public void startBundle() {
            bundleInsertTs = DT_INS.format(Instant.now());
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            FileIO.ReadableFile file = c.element();
            String gcsUri = file.getMetadata().resourceId().toString();
            String objectName = objectNameFromUri(gcsUri);

            try (
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                        Channels.newInputStream(file.open()),
                        StandardCharsets.UTF_8))
            ) {
                String line;
                while ((line = reader.readLine()) != null) {
                    processLine(c, objectName, line);
                }
            } catch (Exception e) {
                c.output(DLQ_TAG, safeDlqJson(
                    json,
                    "file_read_error",
                    model,
                    objectName,
                    null,
                    null,
                    e.toString()));
            }
        }

        private void processLine(ProcessContext c, String objectName, String line) {
            try {
                String[] tokens;
                switch (model) {
                    case "raiway":
                    case "cloudfront_legacy":
                        tokens = line.split("\\t", -1);
                        break;
                    default:
                        tokens = line.trim().split("\\s+");
                }

                Map<String, String> parsed = new HashMap<>();
                mapping.forEach((key, index) ->
                    parsed.put(key, index < tokens.length ? tokens[index] : ""));

                parsed.put("_model", model);
                parsed.put("_file_name", objectName);

                String service = ServiceResolver.resolveService(parsed);
                if (service == null || service.isBlank()) {
                    service = "nomatch";
                }
                parsed.put("_service", service);

                if ("nomatch".equals(service)) {
                    c.output(DLQ_TAG, safeDlqJson(
                        json,
                        "service_nomatch",
                        model,
                        objectName,
                        service,
                        line,
                        null));
                    return;
                }

                boolean isOtt = OTT_SERVICES.contains(service);
                boolean isSoip = SOIP_SERVICES.contains(service);
                boolean isLegacy = LEGACY_SERVICES.contains(service);

                // Preserve the existing routing behavior for unsupported service labels.
                if (!isOtt && !isSoip && !isLegacy) {
                    return;
                }

                TableRow row = rowMapper.toTableRow(parsed);
                if (row == null) {
                    return;
                }

                row.set("file_name", objectName);
                row.set("date_insert", bundleInsertTs);

                String dataset = datasetFor(model, service);
                String table = tableFor(service);
                String project = c.getPipelineOptions()
                    .as(DataflowPipelineOptions.class)
                    .getProject();
                String destination = tableSpec(project, dataset, table);

                c.output(BQ_ROWS_TAG, KV.of(destination, row));
            } catch (Exception e) {
                c.output(DLQ_TAG, safeDlqJson(
                    json,
                    "line_processing_error",
                    model,
                    objectName,
                    null,
                    line,
                    e.toString()));
            }
        }
    }

    static class BqErrorToDlqFn extends DoFn<BigQueryStorageApiInsertError, String> {
        private transient ObjectMapper json;

        @Setup
        public void setup() {
            json = new ObjectMapper();
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            BigQueryStorageApiInsertError err = c.element();
            String fileName = null;
            if (err.getRow() != null && err.getRow().get("file_name") != null) {
                fileName = String.valueOf(err.getRow().get("file_name"));
            }

            String rowJson;
            try {
                rowJson = json.writeValueAsString(err.getRow());
            } catch (Exception e) {
                rowJson = String.valueOf(err.getRow());
            }

            c.output(safeDlqJson(
                json,
                "bigquery_insert_error",
                null,
                fileName,
                null,
                rowJson,
                err.getErrorMessage()));
        }
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(CustomPipelineOptions.class);

        options.setStreaming(true);

        Pipeline p = Pipeline.create(options);

        PCollection<PubsubMessage> notifications =
            p.apply("ReadGcsNotifications",
                PubsubIO.readMessagesWithAttributesAndMessageId()
                    .fromSubscription(options.getInputSubscription()));

        PCollectionTuple notificationOutputs =
            notifications.apply("NotificationToGcsUri",
                ParDo.of(new NotificationToUriFn())
                    .withOutputTags(URI_TAG, TupleTagList.of(DLQ_TAG)));

        PCollection<String> uris = notificationOutputs.get(URI_TAG);
        PCollection<String> notificationDlq = notificationOutputs.get(DLQ_TAG);

        PCollection<FileIO.ReadableFile> readableFiles =
            uris.apply("MatchFiles", FileIO.matchAll())
                .apply("ReadGzipFiles",
                    FileIO.readMatches().withCompression(Compression.GZIP));

        PCollectionTuple processed =
            readableFiles.apply("ReadParseAndRouteFiles",
                ParDo.of(new ParseFileFn(options.getModel()))
                    .withOutputTags(BQ_ROWS_TAG, TupleTagList.of(DLQ_TAG)));

        PCollection<KV<String, TableRow>> rowsForBq =
            processed.get(BQ_ROWS_TAG)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()));

        PCollection<String> processingDlq = processed.get(DLQ_TAG);

        WriteResult writeResult =
            rowsForBq.apply("WriteAllToBQ",
                BigQueryIO.<KV<String, TableRow>>write()
                    .to(new DynamicDestinations<KV<String, TableRow>, String>() {
                        @Override
                        public String getDestination(
                            ValueInSingleWindow<KV<String, TableRow>> elem) {
                            return elem.getValue().getKey();
                        }

                        @Override
                        public TableDestination getTable(String dest) {
                            return new TableDestination(dest, null);
                        }

                        @Override
                        public TableSchema getSchema(String dest) {
                            return null;
                        }
                    })
                    .withFormatFunction(KV::getValue)
                    .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                    .withTriggeringFrequency(Duration.standardSeconds(5))
                    .withAutoSharding()
                    .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                    .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                    .withExtendedErrorInfo()
                    .ignoreUnknownValues());

        PCollection<String> bqDlq =
            writeResult.getFailedStorageApiInserts()
                .apply("BqErrorsToDlq", ParDo.of(new BqErrorToDlqFn()));

        PCollectionList.of(notificationDlq)
            .and(processingDlq)
            .and(bqDlq)
            .apply("MergeDlq", Flatten.pCollections())
            .apply("WriteDlqToPubSub",
                PubsubIO.writeStrings().to(options.getDlqTopic()));

        LOG.info("Starting streaming ingestion pipeline");
        p.run();
    }
}
