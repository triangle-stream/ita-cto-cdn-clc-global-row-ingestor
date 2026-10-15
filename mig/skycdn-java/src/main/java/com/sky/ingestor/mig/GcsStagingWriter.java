package com.sky.ingestor.mig;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;

final class GcsStagingWriter {
    private final Storage storage;
    private final String bucket;
    private final String prefix;

    GcsStagingWriter(Storage storage, String bucket, String prefix) {
        this.storage = storage;
        this.bucket = bucket;
        this.prefix = prefix == null ? "pending" : prefix.replaceAll("^/+|/+$", "");
    }

    void stage(
        SkyCdnParser.Destination destination,
        String sourceBucket,
        String sourceObject,
        String sourceGeneration,
        byte[] payload,
        int rows) throws Exception {

        String generation = sourceGeneration == null ? "" : sourceGeneration;
        String identity =
            sourceBucket + "\n" + sourceObject + "\n" + generation + "\n" +
            destination.dataset() + "\n" + destination.table();

        String hash = sha256(identity);
        String objectName =
            prefix + "/" + destination.dataset() + "/" + destination.table() + "/" +
            hash.substring(0, 2) + "/" + hash + ".ndjson";

        BlobInfo blob = BlobInfo.newBuilder(bucket, objectName)
            .setContentType("application/x-ndjson")
            .setMetadata(Map.of(
                "source_bucket", sourceBucket,
                "source_object", sourceObject,
                "source_generation", generation,
                "dataset", destination.dataset(),
                "table", destination.table(),
                "rows", Integer.toString(rows)))
            .build();

        try {
            storage.create(blob, payload, Storage.BlobTargetOption.doesNotExist());
            System.out.printf(
                "Staged destination=%s.%s rows=%d bytes=%d gs://%s/%s%n",
                destination.dataset(), destination.table(), rows, payload.length, bucket, objectName);
        } catch (StorageException e) {
            if (e.getCode() == 412) {
                System.out.printf(
                    "Staging object already exists; treating as success gs://%s/%s%n",
                    bucket, objectName);
                return;
            }
            throw e;
        }
    }

    private static String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
