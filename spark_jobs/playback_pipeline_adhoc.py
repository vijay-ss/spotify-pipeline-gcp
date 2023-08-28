import os
import re
import logging
from datetime import datetime, timedelta

from google.cloud import storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, col, to_json, collect_list, struct, udf, round
)


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
            write_path = f"gs://{bucket_name}/{clean_folder}/{year}/{month}/{day}/{output_subfolder}"
            df.write.mode("overwrite").format("csv").option("path", write_path).save(header=True)
            all_blobs = client.list_blobs(bucket_name, prefix=f"{clean_folder}/{year}/{month}/{day}/{output_subfolder}")
            fileList = [file.name for file in all_blobs if '.csv' in file.name and '/part-' in file.name]
            output_filepath = fileList[0]
            logging.debug(fileList)
            logging.info(f"Output file written to GCS: {write_path}/{output_filepath}")

            renamed_file = f"{year}_{month}_{day}_{output_subfolder}.csv"
            renamed_filepath = f"{clean_folder}/{year}/{month}/{day}/{output_subfolder}/{renamed_file}"
            move_blob(
                bucket_name=bucket_name,
                blob_name=output_filepath,
                destination_bucket_name=bucket_name,
                destination_blob_name=renamed_filepath
            )
        except Exception as e:
            logging.exception(e, stack_info=True)


def parse_albums(df):
    df_items = df.select(explode('items').alias('items')).select('items.*')
    df_tracks = df_items.select("track.album")

    df_album = df_tracks.select(
        col('album.album_type').alias('album_type'),
        col('album.href').alias('album_href'),
        col('album.id').alias('album_id'),
        col('album.name').alias('album_name'),
        col('album.release_date').alias('album_release_date'),
        col('album.release_date_precision').alias('album_release_date_precision'),
        col('album.total_tracks').alias('total_tracks'),
        col('album.type').alias('type'),
        col('album.uri').alias('album_uri')
    )
    df_album = df_album.withColumn("album_release_date", udf_complete_year((col("album_release_date"))))
    df_album = df_album.drop_duplicates()

    print("albums dataframe:")
    df_album.show()

    return df_album


def parse_artists(df):
    df_items = df.select(explode('items').alias('items')).select('items.*')
    df_tracks = df_items.select("track.artists")

    df_artists = df_tracks.select(explode('artists').alias('artists_exploded')) \
        .select(
            col('artists_exploded.external_urls.spotify').alias('artist_spotify_url'),
            col('artists_exploded.href').alias('artist_href'),
            col('artists_exploded.id').alias('artist_id'),
            col('artists_exploded.name').alias('artist_name'),
            col('artists_exploded.uri').alias('artist_uri')
        )

    df_artists = df_artists.drop_duplicates()

    logging.info(f"artists exploded dataframe: {df_artists.count()}, {len(df_artists.columns)}")

    print("artists dataframe:")
    df_artists.printSchema()
    df_artists.show()
     
    return df_artists


def values_from_key(col, key):
    rx = fr'(?<="{key}":").*?(?=")'
    values = re.findall(rx, col)
    strippedText = str(values).replace('[','').replace(']','').replace("'", "")

    return strippedText

def udf_values_from_key(key):
        return udf(lambda col: values_from_key(col, key))


def complete_year(col):
    if len(col) == 4:
        complete_date = f"{col}-12-31"
        return complete_date
    else:
        return col

udf_complete_year = udf(lambda x: complete_year(x))


def bag_artists(df):
    df_items = df.select(explode('items').alias('items')).select('items.*')
    df_tracks = df_items.select("played_at", "track.id", "track.artists")

    artists_exploded = df_tracks.select(
         "*",
         explode('artists').alias('artists_exploded')) \
            .select(
            col('played_at'),
            col('id'),
            col('artists_exploded.href').alias('artist_href'),
            col('artists_exploded.id').alias('artist_id'),
            col('artists_exploded.name').alias('artist_name'),
            col('artists_exploded.uri').alias('artist_uri')
            )

    bagged_artists = artists_exploded.groupby('played_at', 'id') \
        .agg(
            to_json(
                collect_list(
                    struct("artist_name", "artist_id", "artist_uri")
                )
            ).alias('bagged_artists')
        )

    bagged_artists = bagged_artists.withColumn("artist_names", udf_values_from_key("artist_name")(col("bagged_artists")))
    bagged_artists = bagged_artists.withColumn("artist_ids", udf_values_from_key("artist_id")(col("bagged_artists")))

    logging.info(f"Artists aggregated dataframe: {bagged_artists.count()}, {len(bagged_artists.columns)}")
    bagged_artists.show()
    bagged_artists.printSchema()

    return bagged_artists


def parse_tracks(df):
    df_items = df.select(explode('items').alias('items')).select('items.*')

    df_tracks = df_items.select(
            'played_at',
            'track.album',
            'track.artists',
            'track.duration_ms',
            col('track.href').alias('track_href'),
            col('track.id').alias('track_id'),
            col('track.name').alias('track_name'),
            'track.popularity',
            'track.type',
            col('track.uri').alias('track_uri'),
    )

    df_tracks = df_tracks.select(
            "*",
        col('album.id').alias('album_id'),
        col('album.name').alias('album_name'),
        col('album.release_date').alias('album_release_date'),
        col('album.uri').alias('album_uri')
        ).drop('album')
    df_tracks = df_tracks.withColumn("duration_s", round((col('duration_ms') / 1000), 2))
    df_tracks = df_tracks.withColumn("duration_min", round((col('duration_ms') / 60000), 2))
    df_tracks = df_tracks.withColumn("album_release_date", udf_complete_year(col("album_release_date")))

    df_tracks = df_tracks.drop_duplicates()

    return df_tracks


if __name__ == "__main__":

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

    bucket_name = "playback-history"
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

    client = storage.Client()

    bucket = client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=source_folder):
        if blob.name.endswith('playback_hist.json'):
            filepath = blob.name
            year = filepath.split("/")[1]
            month = filepath.split("/")[2]
            day = filepath.split("/")[3]
            print(blob.name)
            print(year, month, day)

            json_file_path = f'gs://{bucket_name}/{source_folder}/{year}/{month}/{day}/{source_filename}'
            logging.info(f"Loading source file: {json_file_path}")
            df = spark.read.json(json_file_path, multiLine=True)
            print("Source file schema:")
            df.printSchema()
            df.show()
            
            try:
                df_albums = parse_albums(df)
                df_artists = parse_artists(df)
                bagged_artists = bag_artists(df)
                df_tracks = parse_tracks(df)
            except Exception as error:
                logging.exception(error, stack_info=True)


            df_playback = df_tracks.join(
                bagged_artists,
                (df_tracks.played_at == bagged_artists.played_at) & (df_tracks.track_id == bagged_artists.id),
                "left"
            ).select(
                df_tracks['*'],
                bagged_artists['artist_names'],
                bagged_artists['artist_ids'],
                bagged_artists['bagged_artists']
                )

            output_cols = [
                'played_at',
                'duration_ms',
                'duration_s',
                'duration_min',
                'track_href',
                'track_id',
                'track_name',
                'track_uri',
                'artist_names',
                'artist_ids',
                'popularity',
                'album_id',
                'album_name',
                'album_release_date',
                'album_uri'
                ]

            df_playback = df_playback.select(*output_cols)

            df_playback = df_playback.drop_duplicates()
            df_playback = df_playback.sort("played_at")

            print("Playback history output file:")
            df_playback.show()
            df_playback.printSchema()

            # Write to GCS
            client = storage.Client()
            write_to_gcs(df_playback, "playback_hist")
            write_to_gcs(df_albums, "albums")
            write_to_gcs(df_artists, "artists")