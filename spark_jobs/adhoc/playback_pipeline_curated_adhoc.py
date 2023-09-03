import os
import re
import logging
import pytz
from datetime import datetime

from google.cloud import storage
from google.cloud import bigquery

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date

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


def delete_if_exists(df, tablename):
    if 'played_at' in df.columns:
        try:
            client = bigquery.Client()
            timestamp_list = df.select('played_at').collect()
            timestamp_list_utc = sorted([row.played_at.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S.%f') for row in timestamp_list])
            timestamp_list_utc_str = '", "'.join(timestamp_list_utc)

            date = df.first()['played_at']
            delete_sql = f'DELETE FROM `{tablename}` WHERE played_at IN ("{timestamp_list_utc_str}");'
            logging.info(f"delete statement: \n {delete_sql}")

            query_job = client.query(delete_sql)
            result = query_job.result()

            return result
        except Exception as error:
                logging.exception(error, stack_info=True)


def filter_playback_df(df, tablename):
    if 'played_at' in df.columns:
        try:

            timestamp_list = df.select('played_at').collect()
            timestamp_list_utc = sorted([row.played_at.astimezone(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S.%f') for row in timestamp_list])
            timestamp_list_utc_str = '", "'.join(timestamp_list_utc)

            sql = f'SELECT * FROM `{tablename}` WHERE played_at IN ("{timestamp_list_utc_str}");'
            logging.info(sql)
            spark.conf.set("viewsEnabled","true")
            spark.conf.set("materializationDataset","spotify_hist")
            bq_df = spark.read.format("bigquery").load(sql)

            try:
                df = df.join(bq_df, ['played_at'], "leftanti")
            except Exception as error:
                logging.exception(error)

            return df

        except Exception as error:
             logging.exception(error, stack_info=True)
    else:
         return df
        


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

    # current_date = datetime.now()
    # day = current_date.day
    # month = current_date.month
    # year = current_date.year
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bucket_name = "playback-history"
    clean_folder = "01_clean_zone"
    curated_folder = "02_curated_zone"
    output_subfolder = "playback_hist"

    logging.info("Current UTC datetime: " + current_datetime)

    client = storage.Client()
    

    bucket = client.bucket(bucket_name)

    # clean_csv_files = sorted([k.name for k in bucket.list_blobs(prefix=clean_folder) if k.name.endswith('.csv') and not '/part-' in k.name])
    # for csv in clean_csv_files:
    #     csv_filepath = f"gs://{bucket_name}/{csv}"
    #     filename = os.path.basename(csv)
    #     output_subfolder = os.path.dirname(csv).split("/")[-1]

    #     df = spark.read.csv(csv_filepath, header=True, inferSchema=True)
    #     df = df.drop_duplicates()
    #     df = df.withColumn("upload_timestamp", current_timestamp())
    #     df = df.select([df.columns[-1]] + df.columns[:-1])
    #     df.show()
    #     df.printSchema()
    #     filepath = os.path.dirname(csv)
    #     year = filepath.split("/")[1]
    #     month = filepath.split("/")[2]
    #     day = filepath.split("/")[3]
    #     write_to_gcs(df, output_subfolder)

    curated_parquet_files = sorted([k.name for k in bucket.list_blobs(prefix=curated_folder) if k.name.endswith('.parquet') and not '/part-' in k.name])
    for parquet in curated_parquet_files:
        parquet_filepath = f"gs://{bucket_name}/{parquet}"
        filename = os.path.basename(parquet)
        output_subfolder = os.path.dirname(parquet).split("/")[-1]

        df = spark.read.parquet(parquet_filepath, header=True, inferSchema=True)

        if "album_release_date" in df.columns:
            try:
                df = df.withColumn("album_release_date", to_date("album_release_date"))
            except Exception as error:
                logging.exception(error, stack_info=True)

        df = df.drop_duplicates()
        df.show()
        df.printSchema()
        
        # r = delete_if_exists(df, f"spotify_hist.{output_subfolder}")
        # logging.info(r)

        try:
            df = filter_playback_df(df, f"spotify_hist.{output_subfolder}")
        except Exception as error:
            logging.exception(error, stack_info=True)
        
        if df.count() > 0:

            logging.info("Uploading to bigquery")
            df.write \
                .format("bigquery") \
                .option("writeMethod", "direct") \
                .mode("append") \
                .save(f"spotify_hist.{output_subfolder}")