# spotify-pipeline-gcp

## Setup Pyspark for local development

In order to access files directly from GCS, download the Cloud Storage Connector for Hadoop from the Google documentation https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
Place the .jar file in your local venv pyspark directory. In this case: `venv/lib/python3.11/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar`

## Build Image
```
gcloud builds submit --config cloudbuild.yaml
```