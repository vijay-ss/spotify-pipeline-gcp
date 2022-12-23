import json
import requests
from datetime import datetime, timedelta

class Refresh_Access_Token:
    def __init__(self, refresh_token: str, base_64: str):
        self._refresh_token = refresh_token
        self._base_64 = base_64
    
    def request_token(self):

        url = "https://accounts.spotify.com/api/token"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token
            }

        headers = {"Authorization": "Basic " + self._base_64}

        response = requests.post(url, data=data, headers=headers)

        response_json = response.json()
        expiry = int(int(response_json["expires_in"]) / 3600)
        expires_at_dtm = datetime.now() + timedelta(hours=expiry)
        print(f"Refresh token expires in {expiry} hour at: {expires_at_dtm}")

        return response_json["access_token"]
