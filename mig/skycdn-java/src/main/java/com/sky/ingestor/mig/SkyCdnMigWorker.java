package com.sky.ingestor.mig;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.ReadChannel;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.json.JSONObject;

public final class SkyCdnMigWorker {
    private static final DateTimeFormatter INSERT_TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final String PROJECT = env("GCP_PROJECT", env("GOOGLE_CLOUD_PROJECT", ""));
    private static final String SUBSCRIPTION = env("PUBSUB_SUBSCRIPTION", "clt-ingestor-skycdn-streaming");
    private static final String EXPECTED_PREFIX = env("EXPECTED_PREFIX", "skycdn/");
    private static final String STAGING_BUCKET = env("STAGING_BUCKET", "");
    private static final String STAGING_PREFIX = env("STAGING_PREFIX", "pending");
    private static final int WORKER_THREADS = Integer.parseInt(env("WORKER_THREADS", "4"));
    private static final long MAX_OUTSTANDING_MESSAGES =
        Long.parseLong(env("MAX_OUTSTANDING_MESSAGES", "4"));
    private static final long MAX_OUTSTANDING_BYTES =
        Long.parseLong(env("MAX_OUTSTANDING_BYTES", "536870912"));

    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
    private static final GcsStagingWriter STAGING =
        new GcsStagingWriter(STORAGE, STAGING_BUCKET, STAGING_PREFIX);

    public static void main(String[] args) throws Exception {
        if (PROJECT.isBlank()) throw new IllegalStateException("GCP_PROJECT is required");
        if (STAGING_BUCKET.isBlank()) throw new IllegalStateException("STAGING_BUCKET is required");

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
        MessageReceiver receiver = SkyCdnMigWorker::receive;

        FlowControlSettings flow = FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(MAX_OUTSTANDING_MESSAGES)
            .setMaxOutstandingRequestBytes(MAX_OUTSTANDING_BYTES)
            .setLimitExceededBehavior(LimitExceededBehavior.Block)
            .build();

        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
            .setFlowControlSettings(flow)
            .setExecutorProvider(
                InstantiatingExecutorProvider.newBuilder()
                    .setExecutorThreadCount(WORKER_THREADS)
                    .build())
            .setParallelPullCount(1)
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                subscriber.stopAsync();
                subscriber.awaitTerminated(30, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }));

        System.out.printf(
            "Starting SkyCDN MIG staging worker project=%s subscription=%s threads=%d " +
            "maxOutstandingMessages=%d stagingBucket=%s stagingPrefix=%s%n",
            PROJECT, SUBSCRIPTION, WORKER_THREADS, MAX_OUTSTANDING_MESSAGES,
            STAGING_BUCKET, STAGING_PREFIX);

        subscriber.startAsync().awaitRunning();
        subscriber.awaitTerminated();
    }

    private static void receive(PubsubMessage message, AckReplyConsumer consumer) {
        long started = System.nanoTime();
        try {
            Map<String, String> attrs = message.getAttributesMap();
            String bucket = attrs.get("bucketId");
            String object = attrs.get("objectId");
            String generation = attrs.get("objectGeneration");
            String eventType = attrs.get("eventType");

            if (eventType != null && !eventType.isBlank() && !"OBJECT_FINALIZE".equals(eventType)) {
                consumer.ack();
                return;
            }
            if (bucket == null || object == null) {
                throw new IllegalArgumentException("missing bucketId/objectId");
            }
            if (!EXPECTED_PREFIX.isBlank() && !object.startsWith(EXPECTED_PREFIX)) {
                System.out.printf("Ignoring object outside expected prefix: %s%n", object);
                consumer.ack();
                return;
            }

            Result result = processObject(bucket, object, generation);
            consumer.ack();
            System.out.println(
                result.toJson()
                    .put("pubsub_message_id", message.getMessageId())
                    .put("callback_elapsed_seconds", round(secondsSince(started))));
        } catch (Exception e) {
            System.err.printf("Failed message id=%s error=%s%n", message.getMessageId(), e);
            e.printStackTrace(System.err);
            consumer.nack();
        }
    }

    static Result processObject(String bucket, String object, String generation) throws Exception {
        long started = System.nanoTime();
        String dateInsert = INSERT_TS.format(Instant.now());

        long downloadStarted = System.nanoTime();
        byte[] bytes = download(bucket, object, generation);
        double downloadSeconds = secondsSince(downloadStarted);

        boolean gzipped =
            bytes.length >= 2 && (bytes[0] & 0xff) == 0x1f && (bytes[1] & 0xff) == 0x8b;
        InputStream raw = new ByteArrayInputStream(bytes);
        InputStream decoded = gzipped ? new GZIPInputStream(raw, 1 << 16) : raw;

        Map<SkyCdnParser.Destination, StageBuffer> buffers = new HashMap<>();
        Map<String, Long> destinationCounts = new HashMap<>();

        long lines = 0;
        long parsed = 0;
        long nomatch = 0;
        long unsupported = 0;
        long malformed = 0;
        long loopStarted = System.nanoTime();

        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), 1 << 20)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                lines++;
                try {
                    SkyCdnParser.Fields f = SkyCdnParser.parse(line);
                    String service = SkyCdnParser.resolveService(f);
                    if ("nomatch".equals(service)) {
                        nomatch++;
                        continue;
                    }

                    SkyCdnParser.Destination destination = SkyCdnParser.destination(service);
                    if (destination == null) {
                        unsupported++;
                        continue;
                    }

                    JSONObject row = SkyCdnParser.toBigQueryJson(f, object, dateInsert);
                    if (row == null) {
                        malformed++;
                        continue;
                    }

                    StageBuffer buffer =
                        buffers.computeIfAbsent(destination, ignored -> new StageBuffer());
                    buffer.write(row);
                    parsed++;
                    destinationCounts.merge(
                        destination.dataset() + "." + destination.table(), 1L, Long::sum);
                } catch (Exception rowError) {
                    malformed++;
                }
            }
        }

        double loopSeconds = secondsSince(loopStarted);

        long stageStarted = System.nanoTime();
        long stagedBytes = 0;
        int stagedFiles = 0;

        for (Map.Entry<SkyCdnParser.Destination, StageBuffer> entry : buffers.entrySet()) {
            StageBuffer buffer = entry.getValue();
            byte[] payload = buffer.bytes();
            if (buffer.rows() == 0) continue;

            STAGING.stage(
                entry.getKey(),
                bucket,
                object,
                generation,
                payload,
                buffer.rows());

            stagedBytes += payload.length;
            stagedFiles++;
        }

        double stageSeconds = secondsSince(stageStarted);

        return new Result(
            bucket,
            object,
            generation,
            bytes.length,
            gzipped,
            lines,
            parsed,
            parsed,
            nomatch,
            unsupported,
            malformed,
            destinationCounts,
            stagedFiles,
            stagedBytes,
            downloadSeconds,
            loopSeconds,
            stageSeconds,
            secondsSince(started));
    }

    private static byte[] download(String bucket, String object, String generation) throws Exception {
        BlobId blobId =
            generation != null && !generation.isBlank()
                ? BlobId.of(bucket, object, Long.parseLong(generation))
                : BlobId.of(bucket, object);

        try (ReadChannel channel = STORAGE.reader(blobId);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            ByteBuffer buffer = ByteBuffer.allocate(1 << 20);
            while (channel.read(buffer) >= 0) {
                buffer.flip();
                out.write(buffer.array(), buffer.position(), buffer.remaining());
                buffer.clear();
            }
            return out.toByteArray();
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static double secondsSince(long nanos) {
        return (System.nanoTime() - nanos) / 1_000_000_000.0;
    }

    private static double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private static final class StageBuffer {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream(1 << 20);
        private final BufferedWriter writer =
            new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8), 1 << 16);
        private int rows;

        void write(JSONObject row) throws Exception {
            writer.write(row.toString());
            writer.newLine();
            rows++;
        }

        int rows() {
            return rows;
        }

        byte[] bytes() throws Exception {
            writer.flush();
            return out.toByteArray();
        }
    }

    record Result(
        String bucket,
        String object,
        String generation,
        long downloadedBytes,
        boolean inputWasGzip,
        long linesRead,
        long parsedRows,
        long stagedRows,
        long nomatchRows,
        long unsupportedRows,
        long malformedRows,
        Map<String, Long> destinations,
        int stagedFiles,
        long stagedBytes,
        double downloadSeconds,
        double loopSeconds,
        double stageUploadSeconds,
        double elapsedSeconds) {

        JSONObject toJson() {
            return new JSONObject()
                .put("bucket", bucket)
                .put("object", object)
                .put("generation", generation == null ? JSONObject.NULL : generation)
                .put("downloaded_bytes", downloadedBytes)
                .put("input_was_gzip", inputWasGzip)
                .put("lines_read", linesRead)
                .put("parsed_rows", parsedRows)
                .put("staged_rows", stagedRows)
                .put("nomatch_rows", nomatchRows)
                .put("unsupported_rows", unsupportedRows)
                .put("malformed_rows", malformedRows)
                .put("destinations", new JSONObject(destinations))
                .put("staged_files", stagedFiles)
                .put("staged_bytes", stagedBytes)
                .put(
                    "timings",
                    new JSONObject()
                        .put("download_seconds", round(downloadSeconds))
                        .put("loop_seconds", round(loopSeconds))
                        .put("stage_upload_seconds", round(stageUploadSeconds)))
                .put("elapsed_seconds", round(elapsedSeconds));
        }
    }
}
