## For testing:
- Clone the repo and put some .gz logs into the Ready bucket;
- Package the jar (it will generate the jar into the target/ dir)
```bash
mvn clean package
```
- Launch the dataflow job (akamai to BQ; other to GCS object):

```bash
java -cp target/ingestor-1.0-SNAPSHOT.jar      com.sky.ingestor.MainPipeline      --runner=DataflowRunner      --project=sky-it-telemetry-clt-dev      --region=europe-west1      --tempLocation=gs://sky-it-telemetry-clt-dev-ready/temp      --stagingLocation=gs://sky-it-telemetry-clt-dev-ready/staging      --inputFilePattern=gs://sky-it-telemetry-clt-dev-ready/CDN_ITA/*.gz      --jobName=uk-log-ingestor-testBQ-$(date +%s) --gcpTempLocation=gs://sky-it-telemetry-clt-dev-ready/temp --bqProject=sky-it-telemetry-clt-dev
```
- The output - at this stage - will be saved as non-gzipped text files into the Ready bucket, test_output prefix.

Since it's a Dev env, we're using the same bucket (Ready) for everything (dataflow temp/stage location, source files/logs, generated output).

**Please note** that this is still a starting point, json models have to be tailored and any other functionality - right now - is not intended to be precise. What we want now is a working code.
