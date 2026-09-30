package com.sky.ingestor.cloudrun;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import org.json.JSONArray;
import org.json.JSONObject;

public final class SkyCdnCloudRun {
    private static final DateTimeFormatter INSERT_TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private static final String PROJECT = env("BQ_PROJECT", env("GOOGLE_CLOUD_PROJECT", ""));
    private static final String EXPECTED_PREFIX = env("EXPECTED_PREFIX", "skycdn/");
    private static final int ROW_BUFFER_ROWS = Integer.parseInt(env("BQ_ROW_BUFFER_ROWS", "5000"));
    private static final int MAX_PENDING = Integer.parseInt(env("BQ_MAX_PENDING_APPENDS", "16"));
    private static final int HTTP_THREADS = Integer.parseInt(env("HTTP_THREADS", "4"));

    private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
    private static final BqWriterManager WRITERS = createWriters();

    private SkyCdnCloudRun() {}

    public static void main(String[] args) throws Exception {
        if (PROJECT.isBlank()) throw new IllegalStateException("BQ_PROJECT is required");

        int port = Integer.parseInt(env("PORT", "8080"));
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        server.createContext("/healthz", SkyCdnCloudRun::health);
        server.createContext("/", SkyCdnCloudRun::ingest);
        server.setExecutor(Executors.newFixedThreadPool(HTTP_THREADS));
        server.start();
        System.out.printf("SkyCDN Java ingestor listening on port %d%n", port);
    }

    private static void health(HttpExchange x) throws IOException {
        send(x, 200, "{\"ok\":true}");
    }

    private static void ingest(HttpExchange x) throws IOException {
        if (!"POST".equalsIgnoreCase(x.getRequestMethod())) {
            send(x, 405, "method not allowed");
            return;
        }

        try {
            String body = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JSONObject envelope = new JSONObject(body);
            JSONObject message = envelope.optJSONObject("message");
            if (message == null) throw new IllegalArgumentException("missing Pub/Sub message");

            JSONObject attrs = message.optJSONObject("attributes");
            String bucket = attrs == null ? null : attrs.optString("bucketId", null);
            String object = attrs == null ? null : attrs.optString("objectId", null);
            String generation = attrs == null ? null : attrs.optString("objectGeneration", null);
            String eventType = attrs == null ? null : attrs.optString("eventType", null);

            if ((bucket == null || object == null) && message.has("data")) {
                byte[] decoded = java.util.Base64.getDecoder().decode(message.getString("data"));
                JSONObject payload = new JSONObject(new String(decoded, StandardCharsets.UTF_8));
                if (bucket == null) bucket = payload.optString("bucket", null);
                if (object == null) object = payload.optString("name", null);
                if (generation == null) generation = payload.optString("generation", null);
            }

            if (eventType != null && !eventType.isBlank() && !"OBJECT_FINALIZE".equals(eventType)) {
                sendNoContent(x);
                return;
            }
            if (bucket == null || object == null) throw new IllegalArgumentException("missing bucket/object");
            if (!EXPECTED_PREFIX.isBlank() && !object.startsWith(EXPECTED_PREFIX)) {
                System.out.printf("Ignoring object outside expected prefix: %s%n", object);
                sendNoContent(x);
                return;
            }

            Result result = processObject(bucket, object, generation);
            System.out.println(result.toJson());
            sendNoContent(x);
        } catch (Exception e) {
            e.printStackTrace(System.err);
            send(x, 500, "ingestion failed; Pub/Sub should retry");
        }
    }

    static Result processObject(String bucket, String object, String generation) throws Exception {
        long started = System.nanoTime();
        String dateInsert = INSERT_TS.format(Instant.now());

        long t = System.nanoTime();
        byte[] bytes = download(bucket, object, generation);
        double downloadSeconds = secondsSince(t);

        boolean gzipped = bytes.length >= 2 && (bytes[0] & 0xff) == 0x1f && (bytes[1] & 0xff) == 0x8b;
        InputStream raw = new ByteArrayInputStream(bytes);
        InputStream decoded = gzipped ? new GZIPInputStream(raw, 1 << 16) : raw;

        Map<SkyCdnParser.Destination, JSONArray> buffers = new HashMap<>();
        ArrayDeque<BqWriterManager.Pending> pending = new ArrayDeque<>();
        Map<String, Long> destinationCounts = new HashMap<>();

        long lines = 0, parsed = 0, written = 0, nomatch = 0, unsupported = 0, malformed = 0;
        long bqSubmitNanos = 0, bqWaitNanos = 0;
        long loopStarted = System.nanoTime();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(decoded, StandardCharsets.UTF_8), 1 << 20)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                lines++;
                try {
                    String[] f = SkyCdnParser.split(line);
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

                    JSONArray buffer = buffers.computeIfAbsent(destination, d -> new JSONArray());
                    buffer.put(row);
                    parsed++;

                    if (buffer.length() >= ROW_BUFFER_ROWS) {
                        long submitStart = System.nanoTime();
                        BqWriterManager.Pending p = WRITERS.submit(destination.dataset(), destination.table(), buffer);
                        bqSubmitNanos += System.nanoTime() - submitStart;
                        if (p != null) pending.addLast(p);
                        buffers.put(destination, new JSONArray());

                        if (pending.size() >= MAX_PENDING) {
                            BqWriterManager.Pending done = pending.removeFirst();
                            long waitStart = System.nanoTime();
                            done.future().get();
                            bqWaitNanos += System.nanoTime() - waitStart;
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
            long submitStart = System.nanoTime();
            BqWriterManager.Pending p = WRITERS.submit(e.getKey().dataset(), e.getKey().table(), e.getValue());
            bqSubmitNanos += System.nanoTime() - submitStart;
            if (p != null) pending.addLast(p);
        }

        double loopSeconds = secondsSince(loopStarted);
        long finalWaitStart = System.nanoTime();
        while (!pending.isEmpty()) {
            BqWriterManager.Pending done = pending.removeFirst();
            long waitStart = System.nanoTime();
            done.future().get();
            bqWaitNanos += System.nanoTime() - waitStart;
            written += done.rows();
            destinationCounts.merge(done.destination(), (long) done.rows(), Long::sum);
        }
        double finalWaitSeconds = secondsSince(finalWaitStart);
        double elapsedSeconds = secondsSince(started);

        return new Result(bucket, object, generation, bytes.length, gzipped, lines, parsed, written,
            nomatch, unsupported, malformed, destinationCounts, downloadSeconds, loopSeconds,
            bqSubmitNanos / 1_000_000_000.0, bqWaitNanos / 1_000_000_000.0,
            finalWaitSeconds, elapsedSeconds);
    }

    private static byte[] download(String bucket, String object, String generation) {
        if (generation != null && !generation.isBlank()) {
            return STORAGE.readAllBytes(BlobId.of(bucket, object, Long.parseLong(generation)));
        }
        return STORAGE.readAllBytes(bucket, object);
    }

    private static BqWriterManager createWriters() {
        try {
            return new BqWriterManager(PROJECT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static double secondsSince(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    private static void sendNoContent(HttpExchange x) throws IOException {
        x.sendResponseHeaders(204, -1);
        x.close();
    }

    private static void send(HttpExchange x, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        x.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        x.sendResponseHeaders(code, bytes.length);
        x.getResponseBody().write(bytes);
        x.close();
    }

    record Result(
        String bucket, String object, String generation, long downloadedBytes, boolean inputWasGzip,
        long linesRead, long parsedRows, long writtenRows, long nomatchRows, long unsupportedRows,
        long malformedRows, Map<String, Long> destinations, double downloadSeconds, double loopSeconds,
        double bqSubmitSeconds, double bqWaitSeconds, double finalBqWaitSeconds, double elapsedSeconds) {

        String toJson() {
            JSONObject timings = new JSONObject()
                .put("download_seconds", round(downloadSeconds))
                .put("loop_seconds", round(loopSeconds))
                .put("bq_submit_seconds", round(bqSubmitSeconds))
                .put("bq_wait_seconds", round(bqWaitSeconds))
                .put("final_bq_wait_seconds", round(finalBqWaitSeconds));

            return new JSONObject()
                .put("bucket", bucket)
                .put("object", object)
                .put("generation", generation == null ? JSONObject.NULL : generation)
                .put("downloaded_bytes", downloadedBytes)
                .put("input_was_gzip", inputWasGzip)
                .put("lines_read", linesRead)
                .put("parsed_rows", parsedRows)
                .put("written_rows", writtenRows)
                .put("nomatch_rows", nomatchRows)
                .put("unsupported_rows", unsupportedRows)
                .put("malformed_rows", malformedRows)
                .put("destinations", new JSONObject(destinations))
                .put("timings", timings)
                .put("elapsed_seconds", round(elapsedSeconds))
                .toString();
        }

        private static double round(double value) {
            return Math.round(value * 1000.0) / 1000.0;
        }
    }
}
