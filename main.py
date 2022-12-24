import os
import json
import logging
from datetime import datetime

from gcp_utils.common_functions import upload_blob, credentials_accessor
from spotify_api.auth import SpotifyAuth
from spotify_api.spotify_api import PlaybackHistory

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_time = datetime.now()
    credentials = credentials_accessor()
    bucket_name = credentials.get('BUCKET_NAME')
    file_download_location = 'spotify_hist'
    source_folder = '00_source'

    auth = SpotifyAuth(credentials.get('REFRESH_TOKEN'), credentials.get('CLIENT_ID'), credentials.get('CLIENT_SECRET'))
    access_token = auth.request_token()

    api = PlaybackHistory(access_token)
    j = api.get_playback_history()

    track_ids = api.extract_track_id(j)
    artist_ids = api.extract_artist_id(j)

    j_feat = api.get_track_features(track_ids)
    j_genres = api.get_track_genres(artist_ids)

    os.makedirs(file_download_location, exist_ok=True)
    
    with open(f'{file_download_location}/playback_hist.json', 'w') as f:
        f.write(json.dumps(j, indent=4, sort_keys=True))

    with open(f'{file_download_location}/track_genres.json', 'w') as f:
        f.write(json.dumps(j_genres, indent=4, sort_keys=True))

    with open(f'{file_download_location}/track_features.json', 'w') as f:
        f.write(json.dumps(j_feat, indent=4, sort_keys=True))

    current_day = datetime.now().day
    current_month = datetime.now().month
    current_year = datetime.now().year
    current_date = datetime.now().strftime('%Y-%m-%d')

    upload_string = f"{current_year}/{current_month}/{current_day}"

    for file in os.listdir(file_download_location + '/'):
        upload_blob(bucket_name, os.path.join(file_download_location, file), f'{source_folder}/{upload_string}/{file}')
    
    end_time = datetime.now()

    logging.info('Upload to GCS complete.')
    logging.info(f'Execution time: {end_time - start_time}')
