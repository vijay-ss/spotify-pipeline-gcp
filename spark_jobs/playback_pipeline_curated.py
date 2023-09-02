import os
import re
import logging
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def move_blob(
        bucket_name: str,
        blob_name: str,
        destination_bucket_name: str,
        destination_blob_name: str,
        delete_source_file=False
    ):
        """Moves a blob from one bucket to another with a new name.
        
        Args:
            bucket_name: ID of GCS bucket
            blob_name: ID of GCS object
            destination_bucket_name: ID of the bucket to move the object to
            destination_blob_name: ID of new GCS object

        Optional: set a generation-match precondition to avoid potential race conditions
        and data corruptions. The request is aborted if the object's
        generation number does not match your precondition. For a destination
        object that does not yet exist, set the if_generation_match precondition to 0.
        If the destination object already exists in your bucket, set instead a
        generation-match precondition using its generation number.
        """

        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)

        destination_generation_match_precondition = 0

        try:

            blob_copy = source_bucket.copy_blob(
                source_blob, destination_bucket, destination_blob_name, if_generation_match=destination_generation_match_precondition,
            )

            if delete_source_file is True:
                source_bucket.delete_blob(blob_name)
                logging.info(f"Deleted blob: {blob_name}")

            logging.info(
                "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                    source_blob.name,
                    source_bucket.name,
                    blob_copy.name,
                    destination_bucket.name,
                )
            )
        except Exception as e:
            logging.exception(e, stack_info=True)


def write_to_gcs(df, output_subfolder: str) -> None:
        """Write dataframe to GCS bucket. Copies and renames output file
        for human readability.
        """
        try:
            write_path = f"gs://{bucket_name}/{curated_folder}/{year}/{month}/{day}/{output_subfolder}"
            df.write.mode("overwrite").format("parquet").option("path", write_path).save(header=True)
            all_blobs = client.list_blobs(bucket_name, prefix=f"{curated_folder}/{year}/{month}/{day}/{output_subfolder}")  
            fileList = [file.name for file in all_blobs if '.parquet' in file.name and '/part-' in file.name]
            output_filepath = fileList[0]
            logging.debug(fileList)
            logging.info(f"Output file written to GCS: {write_path}/{output_filepath}")

            renamed_file = f"{year}_{month}_{day}_{output_subfolder}.parquet"
            renamed_filepath = f"{curated_folder}/{year}/{month}/{day}/{output_subfolder}/{renamed_file}"
            move_blob(
                bucket_name=bucket_name,
                blob_name=output_filepath,
                destination_bucket_name=bucket_name,
                destination_blob_name=renamed_filepath
            )  
        except Exception as e:
            logging.exception(e, stack_info=True)


if __name__ == "__main__":

    appName = "Spotify Playback History Curated"
    master = "local"
    logging.basicConfig(level=logging.INFO)

    # Create Spark session
    spark = SparkSession.builder \
        .appName(appName) \
        .master(master) \
        .config('spark.jars.packages', 'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.0') \
        .getOrCreate()

    logging.info('PySpark Version: ' + spark.version)

    # Setup hadoop fs configuration for schema gs://
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

    current_date = datetime.now()
    day = current_date.day
    month = current_date.month
    year = current_date.year
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bucket_name = "playback-history"
    clean_folder = "01_clean_zone"
    curated_folder = "02_curated_zone"
    output_subfolder = "playback_hist"

    logging.info("Current UTC datetime: " + current_datetime)

    client = storage.Client()

    bucket = client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=f"{clean_folder}/{year}/{month}/{day}/")
    date_str = f"{year}_{month}_{day}_"
    files = [k.name for k in blobs if date_str in k.name]
    print(files)

    for csv in files:
         csv_filepath = f"gs://{bucket_name}/{csv}"
         filename = os.path.basename(csv)
         output_subfolder = os.path.dirname(csv).split("/")[-1]

         df = spark.read.csv(csv_filepath, header=True, inferSchema=True)
         df = df.withColumn("upload_timestamp", current_timestamp())
         df.show()
         df.printSchema()
         write_to_gcs(df, output_subfolder)

    curated_blobs = bucket.list_blobs(prefix=f"{curated_folder}/{year}/{month}/{day}/")
    parquet_files = [k.name for k in curated_blobs if date_str in k.name]
    print(files)

    for parquet in parquet_files:
        parquet_filepath = f"gs://{bucket_name}/{parquet}"
        filename = os.path.basename(parquet)
        output_subfolder = os.path.dirname(parquet).split("/")[-1]

        df = spark.read.parquet(parquet_filepath, header=True, inferSchema=True)
        df.show()
        df.printSchema()

        def delete_if_exists(df, tablename):
            if 'played_at' in df.columns:
                try:
                    client = bigquery.Client()

                    date = df.first()['played_at']
                    date_str = f"{date.year}-{date.month}-{date.day}"
                    delete_statement = (
                        f'Delete from {tablename} where DATE(played_at) = "{date_str}"'
                    )
                    logging.info(f"delete statement: \n {delete_statement}")

                    query_job = client.query(delete_statement)
                    result = query_job.result()

                    return result
                except Exception as error:
                     logging.exception(error, stack_info=True)
        
        delete_if_exists(df, f"spotify_hist.{output_subfolder}")

        logging.info("Uploading to bigquery")
        df.write \
            .format("bigquery") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save(f"spotify_hist.{output_subfolder}")