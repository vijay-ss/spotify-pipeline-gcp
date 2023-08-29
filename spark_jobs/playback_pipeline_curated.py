import re
import logging
from datetime import datetime, timedelta

from google.cloud import storage

from pyspark.sql import SparkSession


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

    yesterday = datetime.now() - timedelta(1)
    day = yesterday.day
    month = yesterday.month
    year = yesterday.year
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    bucket_name = "playback-history"
    clean_folder = "01_clean_zone"
    output_subfolder = "playback_hist"

    logging.debug("Current UTC datetime: " + current_datetime)

    # json_file_path = f'gs://{bucket_name}/{clean_folder}/{year}/{month}/{day}/{source_filename}'

    client = storage.Client()

    bucket = client.bucket(bucket_name)
    
    blobs = bucket.list_blobs(prefix=f"{clean_folder}/{year}/{month}/{day}/")
    files = [k.name for k in blobs if f"{year}_{month}_{day}_" in k.name]