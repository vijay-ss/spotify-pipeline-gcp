import requests
import datetime

class PlaybackHistory:
    """Connects to Spotify API to pull playback history and track details."""

    def __init__(self, access_token: str):
        self._access_token = access_token


    def get_playback_history(self):
        """Retrieve recent playback history."""

        url = "https://api.spotify.com/v1/me/player/recently-played"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token = self._access_token)
        }

        today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        params = {
            "limit": 50,
            "after": yesterday_unix_timestamp
        }

        response = requests.get(url, headers=headers, params=params)
        response_json = response.json()

        return response_json


    @staticmethod
    def extract_track_id(tracks: dict) -> str:
        """Create a csv list of track ids."""

        data = tracks
        spotify_id = []

        for ids in data["items"]:
            spotify_id.append(ids["track"]["id"])

        track_list = ','.join(spotify_id)

        return track_list

    @staticmethod
    def extract_artist_id(tracks: dict) -> str:
        """Create a csv list of artist ids."""

        data = tracks
        spotify_id = []

        for ids in data["items"]:
            spotify_id.append(ids["track"]["album"]["artists"][0]["id"])

        track_list = ','.join(spotify_id)

        return track_list


    def get_track_features(self, track_ids: str):
        """Takes in a list of track ids and returns song features."""

        get_query = "https://api.spotify.com/v1/audio-features"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token = self._access_token)
        }

        params = {
            "ids": track_ids
        }

        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()

        return response_json


    def get_track_genres(self, track_ids: str):
        """Takes in a list of track ids and returns song genres."""

        get_query = "https://api.spotify.com/v1/artists"

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer {token}".format(token = self._access_token)
        }

        params = {
            "ids": track_ids
        }

        response = requests.get(get_query, headers=headers, params=params)
        response_json = response.json()

        return response_json


    def get_recommendations(self, track_ids: str):

        url = "https://api.spotify.com/v1/recommendations"

        headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer {token}".format(token = self._access_token)
            }

        params = {
            "ids": track_ids
        }

        response = requests.get(url, headers=headers, params=params)
        response_json = response.json()

        return response_json

