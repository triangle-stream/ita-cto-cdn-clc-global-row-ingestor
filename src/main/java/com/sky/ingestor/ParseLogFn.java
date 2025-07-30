package com.sky.ingestor;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;


// Parse log's fields, using spaces or tabs as separators */
public class ParseLogFn extends DoFn<String, KV<String, Map<String, String>>> {
    private final Map<String, Integer> mapping;

    public ParseLogFn(Map<String, Integer> mapping) {
        this.mapping = mapping;
    }

    @ProcessElement
    public void processElement(@Element String line, OutputReceiver<KV<String, Map<String, String>>> out) {
        String[] fields = line.trim().split("\\s+");
        Map<String, String> row = new HashMap<>();
        for (Map.Entry<String, Integer> e : mapping.entrySet()) {
            String val = e.getValue() < fields.length ? fields[e.getValue()] : "";
            row.put(e.getKey(), val != null ? val : "");
        }

        String service = ServiceResolver.resolveService(row);
        out.output(KV.of(service, row));
    }
}