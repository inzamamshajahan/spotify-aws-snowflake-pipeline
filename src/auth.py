import requests
from requests.auth import HTTPBasicAuth

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"

def get_spotify_access_token(client_id: str, client_secret: str) -> str:
    """Obtains an access token from the Spotify API."""
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {"grant_type": "client_credentials"}
    
    response = requests.post(SPOTIFY_AUTH_URL, auth=auth, data=data)
    response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
    
    token_info = response.json()
    return token_info["access_token"]