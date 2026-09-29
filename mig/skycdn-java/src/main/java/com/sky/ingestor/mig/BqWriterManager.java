package com.sky.ingestor.mig;

import com.google.api.core.ApiFuture;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONArray;

final class BqWriterManager implements Closeable {
    record Pending(ApiFuture<AppendRowsResponse> future, int rows, String destination) {}

    private final String project;
    private final BigQueryWriteClient client;
    private final Map<String, JsonStreamWriter> writers = new ConcurrentHashMap<>();

    BqWriterManager(String project) throws IOException {
        this.project = project;
        this.client = BigQueryWriteClient.create();
    }

    Pending submit(String dataset, String table, JSONArray rows) throws Exception {
        if (rows.isEmpty()) return null;
        String destination = dataset + "." + table;
        JsonStreamWriter writer = getWriter(dataset, table);
        return new Pending(writer.append(rows), rows.length(), destination);
    }

    private JsonStreamWriter getWriter(String dataset, String table) throws Exception {
        String key = dataset + "." + table;
        JsonStreamWriter existing = writers.get(key);
        if (existing != null) return existing;

        synchronized (writers) {
            existing = writers.get(key);
            if (existing != null) return existing;
            String tableName = TableName.of(project, dataset, table).toString();
            JsonStreamWriter created = JsonStreamWriter
                .newBuilder(tableName, client)
                .setEnableConnectionPool(true)
                .build();
            writers.put(key, created);
            System.out.printf("Opened BigQuery default stream writer for %s%n", key);
            return created;
        }
    }

    @Override
    public void close() throws IOException {
        IOException failure = null;
        for (JsonStreamWriter writer : writers.values()) {
            try { writer.close(); }
            catch (Exception e) { if (failure == null) failure = new IOException(e); }
        }
        client.close();
        if (failure != null) throw failure;
    }
}
