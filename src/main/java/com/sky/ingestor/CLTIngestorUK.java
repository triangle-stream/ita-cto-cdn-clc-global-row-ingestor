package com.sky.ingestor;

// Imports for Java
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.metrics.Counter;          
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
/// Imports for debugging and logging
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.sky.ingestor.utils.RowMapperFactory;

/// To DO: exclude commented lines in logs
 


public class CLTIngestorUK {


    public interface CustomPipelineOptions extends DataflowPipelineOptions {
        @Description("Objects to process (es. gs://sky-it-telemetry-clt-dev-ready/akamai/nowtv/linear/*.gz) from dag list")
        @Validation.Required
        ValueProvider<String> getInputFileList();
        void setInputFileList(ValueProvider<String> value);    
        
        @Description("GCS prefix for nomatch TXT output, e.g. gs://sky-it-telemetry-clt-dev-ready/test-output/ (must end with /)")
        ValueProvider<String> getTxtOutputPrefix();
        void setTxtOutputPrefix(ValueProvider<String> value);
    }

    /// Logging 
    private static final Logger LOG = LoggerFactory.getLogger(CLTIngestorUK.class);   
    private static final Counter PARSED            =
        Metrics.counter(CLTIngestorUK.class, "parsed_rows");

    // OTT Services (skyGO/NowTV/SkyQ)
    private static final Set<String> OTT_SERVICES = Set.of(
        "nowtv_linear", "nowtv_vod",
        "skygo_linear", "skygo_vod"
    );

    // SOIP Services (glass/stream)
    private static final Set<String> SOIP_SERVICES = Set.of(
        "soip_linear", "soip_vod", "soip_ads", "soip_npvr", "soip_ivod"
    );

    // IT LEGACY Services (hip/verdi/vod-stb)
    private static final Set<String> LEGACY_SERVICES = Set.of(
        "hip_linear", "hip_vod",
        "verdi_linear", "verdi_vod",
        "vod-stb"
    );

    // Counters for BigQuery writes
    private static final Counter BQ_FAILED = Metrics.counter(CLTIngestorUK.class, "bq_failed");
    private static final Counter INPUT_FILES_COUNT = Metrics.counter(CLTIngestorUK.class, "input_files_count");

    // Dataset helper and table name helpers
    // dataset = it_<model>_<group>_logs (es. it_akamai_ott_logs)
    private static String datasetFor(String model, String service) {
        final String group;
        if (LEGACY_SERVICES.contains(service)) {
            group = "legacy";
        } else if (SOIP_SERVICES.contains(service)) {
            group = "soip";
        } else {
            group = "ott";
        }
        // es.: it_akamai_ott_logs, it_skycdn_soip_logs, it_skycdn_legacy_logs
        return String.format("it_%s_%s_logs", model, group);
    }
    private static String tableFor(String service) {
        return service.replace('-', '_');   // opzionale, per sicurezza
    }
    private static String tableSpec(String project, String dataset, String table) {
        return project + ":" + dataset + "." + table;
    }


    public static void main(String[] args) {
        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(CustomPipelineOptions.class);

        Pipeline p = Pipeline.create(options);

        // Current date YYYYMMDD
        String dateSuffix = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        PCollection<String> csvFromDag =
            p.apply("CSVFromDAG",
                Create.ofProvider(options.getInputFileList(), StringUtf8Coder.of()));

        // CSV Splitting: split CSV lines into individual URIs
        PCollection<String> uris =
            csvFromDag.apply("SplitCSV", ParDo.of(new DoFn<String, String>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    String csv = c.element();
                    if (csv == null || csv.trim().isEmpty()) return;
                    for (String s : csv.split(",")) {
                        s = s.trim();
                        if (!s.isEmpty()) {
                            c.output(s);
                        }
                    }
                }
            }));

        // LOG & COUNTER: count input files from DAG
        PCollectionView<List<String>> urisView = uris.apply("CollectUris", View.asList());
        p.apply("LogInputFilesOnce", Create.of(0))
         .apply("DoLogInputs", ParDo.of(new DoFn<Integer, Void>() {
             @ProcessElement public void processElement(ProcessContext c) {
                 List<String> list = c.sideInput(urisView);
                 if (list != null) {
                     INPUT_FILES_COUNT.inc(list.size());
                     LOG.info("Input files from DAG ({}): {}", list.size(), String.join(",", list));
                 } else {
                     LOG.info("Input files from DAG: <none>");
                 }
             }
         }).withSideInputs(urisView));

        // Match & Read 
        PCollection<FileIO.ReadableFile> readableFiles = uris
            .apply("MatchAll", FileIO.matchAll())
            .apply("ReadMatches", FileIO.readMatches().withCompression(Compression.GZIP));

        // Extract lines with file_name 
        PCollection<KV<String, String>> linesWithFilename = readableFiles
            .apply("ExtractLines", ParDo.of(new DoFn<FileIO.ReadableFile, KV<String, String>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                    FileIO.ReadableFile file = c.element();
                    String gcsUri = file.getMetadata().resourceId().toString(); // gs://bucket/path/object.gz
                    String objectName = gcsUri;
                    if (gcsUri.startsWith("gs://")) {
                        int idx = gcsUri.indexOf('/', 5);
                        if (idx > 0 && idx + 1 < gcsUri.length()) {
                            objectName = gcsUri.substring(idx + 1);
                        }
                    }
                    try (Scanner scanner = new Scanner(file.open())) {
                        while (scanner.hasNextLine()) {
                            c.output(KV.of(objectName, scanner.nextLine()));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Errore leggendo il file: " + objectName, e);
                    }
                }
            }));

        // Parse lines with model detection
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
                  row.put("_file_name", fname);
                
                  String service = ServiceResolver.resolveService(row);
                  // unrecognized => "nomatch" (used for TXT branch)
                  if (service == null || service.isBlank()) {
                      service = "nomatch";
                  }
                  row.put("_service", service);
                  PARSED.inc();

                  c.output(row);
              }
          }));

    // Create (destSpec, TableRow) for all services and models
    PCollection<KV<String, TableRow>> rowsForBq =
        parsed.apply("ToBQRowsWithDestination", ParDo.of(new DoFn<Map<String,String>, KV<String,TableRow>>() {

            @ProcessElement
            public void processElement(ProcessContext c){
                Map<String,String> m = c.element();

                String model   = m.get("_model");
                String service = m.get("_service");

                // If service or model is null, set to nomatch and write to TXT on bucket
                if (service == null || model == null) return;
                boolean isOtt  = OTT_SERVICES.contains(service);
                boolean isSoip = SOIP_SERVICES.contains(service);
                boolean isLegacy = LEGACY_SERVICES.contains(service);
                if (!isOtt && !isSoip && !isLegacy) return;

                TableRow row = RowMapperFactory.get(model).toTableRow(m);
                if (row == null) return;

                row.set("file_name", m.getOrDefault("_file_name", ""));

                String dataset = datasetFor(model, service);
                String table   = tableFor(service);
                String dest    = tableSpec(c.getPipelineOptions().as(DataflowPipelineOptions.class).getProject(),
                                           dataset, table);

                c.output(KV.of(dest, row));
            }
        }))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()));


    // Write to BigQuery and use dynamic destinations
    WriteResult wr =
        rowsForBq.apply("WriteAllToBQ",
            BigQueryIO.<KV<String,TableRow>>write()
                .to(new DynamicDestinations<KV<String,TableRow>, String>() {
                    @Override
                    public String getDestination(ValueInSingleWindow<KV<String,TableRow>> elem) {
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
                .withFormatFunction(kv -> kv.getValue())
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(CreateDisposition.CREATE_NEVER)
                .withExtendedErrorInfo()
                .ignoreUnknownValues()  // safe to ignore unknown fields
        );

    // Log errors from BigQuery Storage API
    wr.getFailedStorageApiInserts()
      .apply("CountAndLogBQFails", ParDo.of(new DoFn<BigQueryStorageApiInsertError, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                BQ_FAILED.inc();
                LOG.error("BQ rejected row: {}", c.element().getErrorMessage());
            }
      }));

        // Write to TXT files - nomatch only
        PCollection<Map<String,String>> nomatch =
            parsed.apply("FilterTxtNomatchOnly", Filter.by(m -> "nomatch".equals(m.get("_service"))));

        PCollection<KV<String, String>> nomatchByModel =
            nomatch.apply("ToKVModelLine", MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                .via(m -> KV.of(m.getOrDefault("_model", "unknown"), m.toString())));

        PCollectionList<KV<String,String>> partsKV =
            nomatchByModel.apply("PartitionByModel",
                Partition.of(4, (Partition.PartitionFn<KV<String,String>>) (kv, numPartitions) -> {
                  String model = kv.getKey();
                  if ("akamai".equals(model))     return 0;
                  if ("cloudfront".equals(model)) return 1;
                  if ("skycdn".equals(model))     return 2;
                  if ("raiway".equals(model))     return 3;  // TODO when raiway lands
                  return 3; // fallback
                }));
            
        PCollection<String> akamaiLines    = partsKV.get(0)
            .apply("DropKeyAkamai",    MapElements.into(TypeDescriptors.strings()).via(KV::getValue));
        PCollection<String> cloudfrontLines = partsKV.get(1)
            .apply("DropKeyCloudFront",MapElements.into(TypeDescriptors.strings()).via(KV::getValue));
        PCollection<String> skycdnLines     = partsKV.get(2)
            .apply("DropKeySkyCDN",    MapElements.into(TypeDescriptors.strings()).via(KV::getValue));
        PCollection<String> raiwayLines     = partsKV.get(3)
            .apply("DropKeyRaiway",    MapElements.into(TypeDescriptors.strings()).via(KV::getValue));
            
        PCollectionList<String> parts = PCollectionList.of(akamaiLines)
            .and(cloudfrontLines)
            .and(skycdnLines)
            .and(raiwayLines);
            
        ValueProvider<String> basePrefix = options.getTxtOutputPrefix(); // must end with '/'
            
        // For each model, build the file prefix like: <base>/akamai_nomatch_<YYYYMMDD>
        ValueProvider<String> akamaiPrefix = ValueProvider.NestedValueProvider.of(
            basePrefix, (String b) -> (b.endsWith("/") ? b : b + "/") + "akamai_nomatch_" + dateSuffix);
        ValueProvider<String> cloudfrontPrefix = ValueProvider.NestedValueProvider.of(
            basePrefix, (String b) -> (b.endsWith("/") ? b : b + "/") + "cloudfront_nomatch_" + dateSuffix);
        ValueProvider<String> skycdnPrefix = ValueProvider.NestedValueProvider.of(
            basePrefix, (String b) -> (b.endsWith("/") ? b : b + "/") + "skycdn_nomatch_" + dateSuffix);
        ValueProvider<String> raiwayPrefix = ValueProvider.NestedValueProvider.of(
            basePrefix, (String b) -> (b.endsWith("/") ? b : b + "/") + "raiway_nomatch_" + dateSuffix);
            
        // Write each partition only if it has elements;
        parts.get(0).apply("WriteTXT_akamai",
            TextIO.write()
                  .to(akamaiPrefix)
                  .withSuffix(".txt.gz")
                  .withCompression(Compression.GZIP)
                  .withNumShards(1)
                  .withShardNameTemplate("-SSSS-of-NNNN"));
            
        parts.get(1).apply("WriteTXT_cloudfront",
            TextIO.write()
                  .to(cloudfrontPrefix)
                  .withSuffix(".txt.gz")
                  .withCompression(Compression.GZIP)
                  .withNumShards(1)
                  .withShardNameTemplate("-SSSS-of-NNNN"));
            
        parts.get(2).apply("WriteTXT_skycdn",
            TextIO.write()
                  .to(skycdnPrefix)
                  .withSuffix(".txt.gz")
                  .withCompression(Compression.GZIP)
                  .withNumShards(1)
                  .withShardNameTemplate("-SSSS-of-NNNN"));
            
        parts.get(3).apply("WriteTXT_raiway",
            TextIO.write()
                  .to(raiwayPrefix)
                  .withSuffix(".txt.gz")
                  .withCompression(Compression.GZIP)
                  .withNumShards(1)
                  .withShardNameTemplate("-SSSS-of-NNNN"));
        
        // Run the pipeline
        p.run();        
    }
}
