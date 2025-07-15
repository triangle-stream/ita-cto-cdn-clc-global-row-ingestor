package com.sky.ingestor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MainPipeline {

    public interface CustomPipelineOptions extends DataflowPipelineOptions {
        @Description("Path to input files (e.g. gs://bucket/logs/*.gz)")
        @Validation.Required
        String getInputFilePattern();
        void setInputFilePattern(String value);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CustomPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // Calcola data corrente in formato YYYYMMDD
        String dateSuffix = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // Legge file gzip da input, mantiene filename per rilevare il modello
        PCollection<KV<String, String>> linesWithFilename = p
            .apply("MatchFiles", FileIO.match().filepattern(options.getInputFilePattern()))
            .apply("ReadFiles", FileIO.readMatches().withCompression(Compression.GZIP))
            .apply("ExtractLines", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    FileIO.ReadableFile file = c.element();
                    String filename = file.getMetadata().resourceId().getFilename();
                    try (Scanner scanner = new Scanner(file.open())) {
                        while (scanner.hasNextLine()) {
                            c.output(KV.of(filename, scanner.nextLine()));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Errore leggendo il file: " + filename, e);
                    }
                }
            }));

        // Parsing + model detection + service resolution
        PCollection<KV<String, Map<String, String>>> parsed = linesWithFilename
            .apply("ParseWithModel", ParDo.of(new DoFn<KV<String, String>, KV<String, Map<String, String>>>() {
                private Map<String, Map<String, Integer>> models;

                @Setup
                public void setup() throws Exception {
                    models = ModelResolver.loadAllModels();
                }

                @ProcessElement
                public void processElement(ProcessContext c) {
                    String filename = c.element().getKey();
                    String line = c.element().getValue();

                    String modelKey = ModelResolver.detectModel(filename);
                    if (modelKey == null || !models.containsKey(modelKey)) return;

                    Map<String, Integer> mapping = models.get(modelKey);
                    String[] tokens = line.split(" ");
                    Map<String, String> row = new HashMap<>();

                    for (Map.Entry<String, Integer> entry : mapping.entrySet()) {
                        int index = entry.getValue();
                        String value = (index < tokens.length) ? tokens[index] : "";
                        row.put(entry.getKey(), value);
                    }

                    row.put("_model", modelKey); //model nella riga per usarlo in serviceresolver

                    String service = ServiceResolver.resolveService(row);
                    if (service == null || service.isEmpty()) service = "unknown";

                    String partitionedService = service + "_" + dateSuffix;
                    c.output(KV.of(partitionedService, row));
                }
            }));

        // Output (placeholder: stampa a log)
//        parsed.apply("LogOutput", ParDo.of(new DoFn<KV<String, Map<String, String>>, Void>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) {
//                System.out.println(c.element().getKey() + " => " + c.element().getValue());
//            }
//        }));

//          parsed.apply(FileIO.<String, KV<String, Map<String,String>>>writeDynamic()
//                 .by(KV::getKey)
//                 .via(Contextful.fn(kv -> kv.getValue().toString()), TextIO.sink())
//                 .to("gs://sky-it-telemetry-clt-dev-ready/test-output/")
//                 .withNaming(key -> FileIO.Write.defaultNaming(key, ".txt")));
//                  
//

        parsed.apply(
            FileIO.<String, KV<String, Map<String,String>>>writeDynamic()
                
                // key = partitionedService
                .by(KV::getKey)
                
                // coder esplicito per il tipo String
                .withDestinationCoder(StringUtf8Coder.of())
                
                // scriviamo il valore come testo
                .via(Contextful.fn(kv -> kv.getValue().toString()), TextIO.sink())
                
                // cartella di output (GCS)
                .to("gs://sky-it-telemetry-clt-dev-ready/test-output/")
                
                // nome file per ciascuna “tabella”
                .withNaming(key -> FileIO.Write.defaultNaming(key, ".txt"))
        );
        p.run().waitUntilFinish();
    }
}
