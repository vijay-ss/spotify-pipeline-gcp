import json
import base64
import requests
from datetime import datetime, timedelta

class SpotifyAuth:
    """
    Basic authentication for retrieving access token for making api calls.

    Args:
        refresh_token (str): unique token for refreshing the access_token. 
            https://developer.spotify.com/documentation/ios/guides/token-swap-and-refresh/
        client_id (str): unique spotify developer id.
        client_secret (str): unique spotify developer secret.
    """
    def __init__(self, refresh_token: str, client_id: str, client_secret: str) -> str:
        self.__refresh_token = refresh_token
        auth_string = client_id + ":" + client_secret
        auth_bytes = auth_string.encode("utf-8")
        self.__auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")
    
    def request_token(self):
        """Retrieve access token."""
        url = "https://accounts.spotify.com/api/token"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.__refresh_token
            }

        headers = {"Authorization": "Basic " + self.__auth_base64}

        response = requests.post(url, data=data, headers=headers)

        response_json = response.json()
        expiry = int(int(response_json["expires_in"]) / 3600)
        expires_at_dtm = datetime.now() + timedelta(hours=expiry)
        print(f"Access token expires in {expiry} hour at: {expires_at_dtm}")

        return response_json["access_token"]