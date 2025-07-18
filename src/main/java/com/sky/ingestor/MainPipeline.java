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
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import com.sky.ingestor.utils.RowMapperFactory;  

/// Un po' di log non fa male
import org.apache.beam.sdk.metrics.Counter;          
import org.apache.beam.sdk.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// To DO: exclude commented lines in logs
 


public class MainPipeline {


    public interface CustomPipelineOptions extends DataflowPipelineOptions {
        @Description("Path to input files (e.g. gs://bucket/logs/*.gz)")
        @Validation.Required
        String getInputFilePattern();
        void setInputFilePattern(String value);

        @Description("BigQuery project (es. sky-it-telemetry-clt-dev)")
        @Validation.Required
        String getBqProject();
        void setBqProject(String value);

    }

    /// inizia aggiunta per log
    private static final Logger LOG = LoggerFactory.getLogger(MainPipeline.class);   
    private static final Counter PARSED            =
        Metrics.counter(MainPipeline.class, "parsed_rows");
    private static final Counter AKA_NOWTV_TO_BQ   =
        Metrics.counter(MainPipeline.class, "aka_nowtv_rows_to_bq");
    /// finisce aggiunta per log


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
        PCollection<Map<String,String>> parsed = linesWithFilename
          .apply("ParseWithModel", ParDo.of(new DoFn<KV<String,String>, Map<String,String>>() {
              private Map<String, Map<String,Integer>> models;
        
              @Setup public void setup() throws Exception {
                  models = ModelResolver.loadAllModels();
              }
          
              @ProcessElement
              public void processElement(ProcessContext c){
                  String fname = c.element().getKey();
                  String line  = c.element().getValue();
            
                  String model = ModelResolver.detectModel(fname);
                  if(model==null || !models.containsKey(model)) return;
            
                  Map<String,Integer> mapping = models.get(model);
                  String[] toks = line.trim().split("\\s+");
                  Map<String,String> row = new HashMap<>();
                  mapping.forEach((k,idx)-> row.put(k, idx<toks.length ? toks[idx] : ""));
            
                  row.put("_model", model);
                
                  String service = ServiceResolver.resolveService(row);
                  row.put("_service", service!=null&&!service.isEmpty()?service:"unknown");
            
                  PARSED.inc(); // per loggare parsed

                  c.output(row);
              }
          }));


        // Output stampato a log)
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
        String dataSetAkamai = "it_akamai_ott_logs";               // hard-coded per ora
        String tableSpecBase = String.format("%s:%s.",                 // project:dataset.
                                options.getBqProject(), dataSetAkamai);

        parsed
            .apply("FilterAkamaiLinear",
                   Filter.by(m -> "akamai".equals(m.get("_model"))
                                && "nowtv_linear".equals(m.get("_service"))))
//// TEMP FOR LOG            .apply("ToBQrow",
//// TEMP FOR LOG                   MapElements.into(TypeDescriptor.of(TableRow.class))
//// TEMP FOR LOG                              .via(m -> RowMapperFactory.get("akamai").toTableRow(m)))

            .apply("ToBQrow",
                   MapElements.into(TypeDescriptor.of(TableRow.class))
                      .via(m -> {
                          TableRow r = RowMapperFactory.get("akamai").toTableRow(m);
                          if (r != null) { 
                            /// LOGG
                              AKA_NOWTV_TO_BQ.inc();    
                          }
                          return r;
                      }))
            .apply("DropNull", Filter.by(t -> t != null))               // solo righe valide
            
            .apply("WriteBQ",
                   BigQueryIO.writeTableRows()
                      .to(tableSpecBase + "nowtv_linear")
                      .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                      .ignoreUnknownValues()      // opzionale se restano campi
        );

        parsed
          .apply("FilterTxt",
                 Filter.by(m -> !( "akamai".equals(m.get("_model"))
                                && "nowtv_linear".equals(m.get("_service")) )))
          // assegna la chiave (destinazione) senza cambiare il valore
          .apply("AddKey",
                 WithKeys.of((Map<String,String> m) ->
                       m.get("_model") + "_" + m.get("_service") + "_" + dateSuffix))
          // utf8 ---- serve un coder esplicito per Map<String,String>
          .setCoder(KvCoder.of(StringUtf8Coder.of(),
                               MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))

          // scrittura dinamica su GCS
          .apply("WriteTxt",
                 FileIO.<String, KV<String,Map<String,String>>>writeDynamic()
                   .by(KV::getKey)                                 // destinazione
                   .withDestinationCoder(StringUtf8Coder.of())
                   .via(Contextful.fn(kv -> kv.getValue().toString()), TextIO.sink())
                   .to("gs://sky-it-telemetry-clt-dev-ready/test-output/")
                   .withNaming(key -> FileIO.Write.defaultNaming(key, ".txt")));


        p.run().waitUntilFinish();
    }
}
