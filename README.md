## For testing:
- Clone the repo and put some .gz logs into the Ready bucket;
- Package the jar (it will generate the jar into the target/ dir)
```bash
mvn clean package
```
- Launch the dataflow job (akamai to BQ; other to GCS object):

```bash
java -cp target/ingestor-1.0-SNAPSHOT.jar      com.sky.ingestor.MainPipeline      --runner=DataflowRunner      --project=sky-it-telemetry-clt-dev      --region=europe-west1      --tempLocation=gs://sky-it-telemetry-clt-dev-ready/temp      --stagingLocation=gs://sky-it-telemetry-clt-dev-ready/staging      --inputFilePattern=gs://sky-it-telemetry-clt-dev-ready/CDN_ITA/*.gz      --jobName=uk-log-ingestor-testBQ-$(date +%s) --gcpTempLocation=gs://sky-it-telemetry-clt-dev-ready/temp --bqProject=sky-it-telemetry-clt-dev

or

java -cp target/ingestor-1.0-SNAPSHOT.jar      com.sky.ingestor.MainPipeline      
--runner=DataflowRunner     
--project=sky-it-telemetry-clt-dev     
--region=europe-west1     
--tempLocation=gs://sky-it-telemetry-clt-dev-ready/temp      
--stagingLocation=gs://sky-it-telemetry-clt-dev-ready/staging      
--inputFilePattern=gs://sky-it-telemetry-clt-dev-ready/CDN_ITA/*.gz      
--bqProject=sky-it-telemetry-clt-dev      
--jobName=uk-log-ingestor-testBQ-$(date +%s)

```

## Cloudbuild build and jar upload:

```bash

gcloud builds submit   --config cloudbuild.yaml --project=sky-it-telemetry-clt-stage   --substitutions _PROJECT=sky-it-telemetry-clt-stage,_REGION=europe-west1,_VERSION=0.1.0

```bash
