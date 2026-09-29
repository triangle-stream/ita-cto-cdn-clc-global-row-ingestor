package com.sky.ingestor.mig;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.InstantiatingExecutorProvider;
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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.json.JSONArray;
import org.json.JSONObject;

public final class SkyCdnMigWorker {
    private static final DateTimeFormatter INSERT_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final String PROJECT = env("GCP_PROJECT", env("GOOGLE_CLOUD_PROJECT", ""));
    private static final String BQ_PROJECT = env("BQ_PROJECT", PROJECT);
    private static final String SUBSCRIPTION = env("PUBSUB_SUBSCRIPTION", "clt-ingestor-skycdn-streaming");
    private static final String EXPECTED_PREFIX = env("EXPECTED_PREFIX", "skycdn/");
    private static final int WORKER_THREADS = Integer.parseInt(env("WORKER_THREADS", "4"));
    private static final long MAX_OUTSTANDING_MESSAGES = Long.parseLong(env("MAX_OUTSTANDING_MESSAGES", "8"));
    private static final long MAX_OUTSTANDING_BYTES = Long.parseLong(env("MAX_OUTSTANDING_BYTES", "536870912"));
    private static final int ROW_BUFFER_ROWS = Integer.parseInt(env("BQ_ROW_BUFFER_ROWS", "5000"));
    private static final int MAX_PENDING = Integer.parseInt(env("BQ_MAX_PENDING_APPENDS", "16"));

    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
    private static final BqWriterManager WRITERS = createWriters();

    public static void main(String[] args) throws Exception {
        if (PROJECT.isBlank()) throw new IllegalStateException("GCP_PROJECT is required");

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION);
        MessageReceiver receiver = SkyCdnMigWorker::receive;

        FlowControlSettings flow = FlowControlSettings.newBuilder()
            .setMaxOutstandingElementCount(MAX_OUTSTANDING_MESSAGES)
            .setMaxOutstandingRequestBytes(MAX_OUTSTANDING_BYTES)
            .setLimitExceededBehavior(FlowControlSettings.LimitExceededBehavior.Block)
            .build();

        Subscriber subscriber = Subscriber.newBuilder(subscriptionName, receiver)
            .setFlowControlSettings(flow)
            .setExecutorProvider(InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(WORKER_THREADS).build())
            .setParallelPullCount(Math.max(1, Math.min(WORKER_THREADS, 4)))
            .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                subscriber.stopAsync();
                subscriber.awaitTerminated(30, TimeUnit.SECONDS);
                WRITERS.close();
            } catch (Exception ignored) {}
        }));

        System.out.printf("Starting SkyCDN MIG worker project=%s subscription=%s threads=%d maxOutstandingMessages=%d%n",
            PROJECT, SUBSCRIPTION, WORKER_THREADS, MAX_OUTSTANDING_MESSAGES);

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
            if (bucket == null || object == null) throw new IllegalArgumentException("missing bucketId/objectId");
            if (!EXPECTED_PREFIX.isBlank() && !object.startsWith(EXPECTED_PREFIX)) {
                System.out.printf("Ignoring object outside expected prefix: %s%n", object);
                consumer.ack();
                return;
            }

            Result result = processObject(bucket, object, generation);
            consumer.ack();
            System.out.println(result.toJson().put("pubsub_message_id", message.getMessageId()).put("callback_elapsed_seconds", round(secondsSince(started))));
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

        boolean gzipped = bytes.length >= 2 && (bytes[0] & 0xff) == 0x1f && (bytes[1] & 0xff) == 0x8b;
        InputStream raw = new ByteArrayInputStream(bytes);
        InputStream decoded = gzipped ? new GZIPInputStream(raw, 1 << 16) : raw;

        Map<SkyCdnParser.Destination, JSONArray> buffers = new HashMap<>();
        ArrayDeque<BqWriterManager.Pending> pending = new ArrayDeque<>();
        Map<String, Long> destinationCounts = new HashMap<>();

        long lines = 0, parsed = 0, written = 0, nomatch = 0, unsupported = 0, malformed = 0;
        long bqSubmitNanos = 0, bqWaitNanos = 0;
        long loopStarted = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8), 1 << 20)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                lines++;
                try {
                    String[] f = SkyCdnParser.split(line);
                    String service = SkyCdnParser.resolveService(f);
                    if ("nomatch".equals(service)) { nomatch++; continue; }
                    SkyCdnParser.Destination destination = SkyCdnParser.destination(service);
                    if (destination == null) { unsupported++; continue; }
                    JSONObject row = SkyCdnParser.toBigQueryJson(f, object, dateInsert);
                    if (row == null) { malformed++; continue; }

                    JSONArray buffer = buffers.computeIfAbsent(destination, d -> new JSONArray());
                    buffer.put(row);
                    parsed++;

                    if (buffer.length() >= ROW_BUFFER_ROWS) {
                        long submitStarted = System.nanoTime();
                        BqWriterManager.Pending p = WRITERS.submit(destination.dataset(), destination.table(), buffer);
                        bqSubmitNanos += System.nanoTime() - submitStarted;
                        if (p != null) pending.addLast(p);
                        buffers.put(destination, new JSONArray());
                        if (pending.size() >= MAX_PENDING) {
                            BqWriterManager.Pending done = pending.removeFirst();
                            long waitStarted = System.nanoTime();
                            done.future().get();
                            bqWaitNanos += System.nanoTime() - waitStarted;
                            written += done.rows();
                            destinationCounts.merge(done.destination(), (long) done.rows(), Long::sum);
                        }
                    }
                } catch (Exception rowError) {
                    malformed++;
                }
            }
        }

        for (Map.Entry<SkyCdnParser.Destination, JSONArray> e : buffers.entrySet()) {
            if (e.getValue().isEmpty()) continue;
            long submitStarted = System.nanoTime();
            BqWriterManager.Pending p = WRITERS.submit(e.getKey().dataset(), e.getKey().table(), e.getValue());
            bqSubmitNanos += System.nanoTime() - submitStarted;
            if (p != null) pending.addLast(p);
        }

        double loopSeconds = secondsSince(loopStarted);
        long finalWaitStarted = System.nanoTime();
        while (!pending.isEmpty()) {
            BqWriterManager.Pending done = pending.removeFirst();
            long waitStarted = System.nanoTime();
            done.future().get();
            bqWaitNanos += System.nanoTime() - waitStarted;
            written += done.rows();
            destinationCounts.merge(done.destination(), (long) done.rows(), Long::sum);
        }
        double finalWaitSeconds = secondsSince(finalWaitStarted);

        return new Result(bucket, object, generation, bytes.length, gzipped, lines, parsed, written, nomatch,
            unsupported, malformed, destinationCounts, downloadSeconds, loopSeconds,
            bqSubmitNanos / 1_000_000_000.0, bqWaitNanos / 1_000_000_000.0,
            finalWaitSeconds, secondsSince(started));
    }

    private static byte[] download(String bucket, String object, String generation) {
        if (generation != null && !generation.isBlank()) return STORAGE.readAllBytes(BlobId.of(bucket, object, Long.parseLong(generation)));
        return STORAGE.readAllBytes(bucket, object);
    }

    private static BqWriterManager createWriters() {
        try { return new BqWriterManager(BQ_PROJECT); }
        catch (Exception e) { throw new RuntimeException(e); }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static double secondsSince(long nanos) { return (System.nanoTime() - nanos) / 1_000_000_000.0; }
    private static double round(double value) { return Math.round(value * 1000.0) / 1000.0; }

    record Result(String bucket, String object, String generation, long downloadedBytes, boolean inputWasGzip,
                  long linesRead, long parsedRows, long writtenRows, long nomatchRows, long unsupportedRows,
                  long malformedRows, Map<String, Long> destinations, double downloadSeconds, double loopSeconds,
                  double bqSubmitSeconds, double bqWaitSeconds, double finalBqWaitSeconds, double elapsedSeconds) {
        JSONObject toJson() {
            return new JSONObject()
                .put("bucket", bucket).put("object", object)
                .put("generation", generation == null ? JSONObject.NULL : generation)
                .put("downloaded_bytes", downloadedBytes).put("input_was_gzip", inputWasGzip)
                .put("lines_read", linesRead).put("parsed_rows", parsedRows).put("written_rows", writtenRows)
                .put("nomatch_rows", nomatchRows).put("unsupported_rows", unsupportedRows).put("malformed_rows", malformedRows)
                .put("destinations", new JSONObject(destinations))
                .put("timings", new JSONObject()
                    .put("download_seconds", round(downloadSeconds))
                    .put("loop_seconds", round(loopSeconds))
                    .put("bq_submit_seconds", round(bqSubmitSeconds))
                    .put("bq_wait_seconds", round(bqWaitSeconds))
                    .put("final_bq_wait_seconds", round(finalBqWaitSeconds)))
                .put("elapsed_seconds", round(elapsedSeconds));
        }
    }
}
