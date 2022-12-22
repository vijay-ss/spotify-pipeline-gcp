import json
from datetime import datetime

from gcp_utils.upload_to_gcs import upload_blob, credentials_accessor
from spotify_api.refresh_token import Refresh_Access_Token
from spotify_api.spotify_api import PlaybackHistory

if __name__ == "__main__":
    credentials = credentials_accessor()
    bucket_name = credentials.get('BUCKET_NAME')

    a = Refresh_Access_Token(credentials.get('REFRESH_TOKEN'), credentials.get('BASE_64'))
    token = a.request_token()

    api = PlaybackHistory(token)
    j = api.get_playback_history()

    track_ids = api.extract_track_id(j)
    artist_ids = api.extract_artist_id(j)

    j_feat = api.get_track_features(track_ids)
    j_genres = api.get_track_genres(artist_ids)
    #j_recommend = api.get_recommendations(track_ids)
    
    with open('playback_hist.json', 'w') as f:
     f.write(json.dumps(j, indent=4, sort_keys=True))

    with open('track_genres.json', 'w') as f:
     f.write(json.dumps(j_genres, indent=4, sort_keys=True))

    with open('track_features.json', 'w') as f:
     f.write(json.dumps(j_feat, indent=4, sort_keys=True))

    current_day = datetime.now().day
    current_month = datetime.now().month
    current_year = datetime.now().year
    current_date = datetime.now().strftime('%Y-%m-%d')

    upload_string = f"{current_year}/{current_month}/{current_day}"

    upload_blob(bucket_name, 'playback_hist.json', f'00_source/{upload_string}/playback_hist.json')
    upload_blob(bucket_name, 'track_genres.json', f'00_source/{upload_string}/track_genres.json')
    upload_blob(bucket_name, 'track_features.json', f'00_source/{upload_string}/track_features.json')

    print('upload complete.')
