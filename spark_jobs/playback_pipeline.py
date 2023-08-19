import re
import logging
from datetime import datetime, timedelta

from google.cloud import storage

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import explode, col, to_json, collect_list, create_map, struct, map_from_entries, get_json_object, udf

appName = "Spotify Playback History"
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

bucket_name = "playback_history"
source_folder = "00_landing_zone"
source_filename = "playback_hist.json"
clean_folder = "01_clean_zone"
output_subfolder = "playback_hist"

yesterday = datetime.now() - timedelta(1)
current_day = yesterday.day
current_month = yesterday.month
current_year = yesterday.year
current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

logging.debug("Current UTC datetime: " + current_datetime)

# Create playback history data frame
json_file_path = f'gs://{bucket_name}/{source_folder}/{current_year}/{current_month}/{current_day}/{source_filename}'
df = spark.read.json(json_file_path, multiLine=True)
df.printSchema()
df.show()

data_items = df.select(explode('items').alias('items')).select('items.*')
print("items schema:")
data_items.printSchema()
data_items.show()

df_playback = data_items.select(
    'played_at',
    'track.album',
    'track.artists',
    'track.duration_ms',
    'track.href',
    'track.id',
    'track.name',
    'track.popularity',
    'track.type',
    'track.uri',
    )

df_tracks = data_items.select(
    "track.*"
    )

df_tracks.printSchema()

print('Tracks DataFrame:')
df_tracks.show()

df_artists = df_tracks.select(
    "*",
    'album.*'
)

df_album = df_tracks.select(
    col('album.album_type').alias('album_type'),
    col('album.href').alias('href'),
    col('album.id').alias('id'),
    col('album.name').alias('name'),
    col('album.release_date').alias('release_date'),
    col('album.release_date_precision').alias('release_date_precision'),
    col('album.total_tracks').alias('total_tracks'),
    col('album.type').alias('type'),
    col('album.uri').alias('uri')
)
print("albums dataframe:")
df_album.show()

df_artists = df_tracks.select(
    explode('artists').alias('artists')
).select('artists.*')

print("Artists DataFrame:")
df_artists.printSchema()
df_artists.show()
print("Albums DataFrame:")
df_album.printSchema()
df_album.show()

df_output = df_playback.select(
    "*",
    col('album.id').alias('album_id'),
    col('album.name').alias('album_name'),
    col('album.release_date').alias('album_release_date'),
    col('album.uri').alias('album_uri'),
)

df_output = df_output.drop('album')
df_output.printSchema()
df_output.show()

# aggregate artists by timestamp
artists_exploded = df_output.select("*", explode('artists').alias('artists_exploded')).select(
col('played_at'),
col('id'),
col('artists_exploded.href').alias('artist_href'),
col('artists_exploded.id').alias('artist_id'),
col('artists_exploded.name').alias('artist_name'),
col('artists_exploded.uri').alias('artist_uri'))

logging.info(f"artists exploded dataframe: {artists_exploded.count()}, {len(artists_exploded.columns)}")

artists_agg = artists_exploded.groupby('played_at', 'id').agg(
  to_json(
    collect_list(
      struct("artist_name", "artist_id", "artist_uri")
    )
  ).alias('bagged_artists')
)
artists_agg.printSchema()

def extract_artist_names(col):
    rx = r'(?<="artist_name":").*?(?=")'
    artists = re.findall(rx, col)
    strippedText = str(artists).replace('[','').replace(']','').replace("'", "")
    return strippedText


extract_artist_names_udf = udf(lambda x: extract_artist_names(x))

artists_agg = artists_agg.withColumn("artist_names", extract_artist_names_udf(col("bagged_artists")))
artists_agg.show()
logging.info(f"artists aggregated dataframe: {artists_agg.count()}, {len(artists_agg.columns)}")


def sparkShape(dataFrame):
    return (dataFrame.count(), len(dataFrame.columns))
pyspark.sql.dataframe.DataFrame.shape = sparkShape
print(artists_agg.shape())

df_output = df_output.join(
    artists_agg,
    (df_output.played_at == artists_agg.played_at) & (df_output.id == artists_agg.id),
    "left"
).select(df_output['*'], artists_agg['artist_names'], artists_agg['bagged_artists'])
output_cols = ['played_at','duration_ms', 'href', 'id', 'name', 'uri', 'artist_names', 'bagged_artists', 'popularity', 'type', 'album_id', 'album_name', 'album_release_date', 'album_uri']
df_output = df_output.select(*output_cols)

df_output = df_output.drop_duplicates()
df_output = df_output.sort("played_at")
df_output.show()
df_output.printSchema()

# Write to GCS

def move_blob(
    bucket_name: str,
    blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
    delete_source_file=False
):
    """Moves a blob from one bucket to another with a new name."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # There is also an `if_source_generation_match` parameter, which is not used in this example.
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
    try:
        write_path = f"gs://{bucket_name}/{clean_folder}/{current_year}/{current_month}/{current_day}/{output_subfolder}"
        df.write.mode("overwrite").format("csv").option("path", write_path).save(header=True)
        all_blobs = client.list_blobs(bucket_name, prefix=f"{clean_folder}/{current_year}/{current_month}/{current_day}/{output_subfolder}")  
        fileList = [file.name for file in all_blobs if '.csv' in file.name and '/part-' in file.name]
        output_filepath = fileList[0]
        logging.debug(fileList)
        logging.info(f"Output file written to GCS: {write_path}/{output_filepath}")

        renamed_file = f"{current_year}_{current_month}_{current_day}_{output_subfolder}.csv"
        renamed_filepath = f"{clean_folder}/{current_year}/{current_month}/{current_day}/{output_subfolder}/{renamed_file}"
        move_blob(
            bucket_name=bucket_name,
            blob_name=output_filepath,
            destination_bucket_name=bucket_name,
            destination_blob_name=renamed_filepath
        )
    except Exception as e:
        logging.exception(e, stack_info=True)


if __name__ == "__main__":

    client = storage.Client()
    write_to_gcs(df_output, "playback_hist")
    write_to_gcs(df_album, "albums")
    write_to_gcs(df_album, "artists")


# loop through each directory using list blobs and get dir names
# add them to a dict and zip them together