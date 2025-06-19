import requests
from typing import List, Dict, Any

SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

def get_new_releases(access_token: str, limit: int = 20) -> List[Dict[str, Any]]:
    """Fetches newly released albums from Spotify."""
    headers = {"Authorization": f"Bearer {access_token}"}
    params = {"limit": limit}
    response = requests.get(f"{SPOTIFY_API_BASE_URL}/browse/new-releases", headers=headers, params=params)
    response.raise_for_status()
    return response.json()["albums"]["items"]

def get_album_tracks(access_token: str, album_id: str) -> List[Dict[str, Any]]:
    """Fetches all tracks for a given album and enriches them with album details."""
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Get album details first to enrich the tracks
    album_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}"
    album_response = requests.get(album_url, headers=headers)
    album_response.raise_for_status()
    album_details = album_response.json()

    # Get tracks for the album
    tracks_url = f"{SPOTIFY_API_BASE_URL}/albums/{album_id}/tracks"
    tracks_response = requests.get(tracks_url, headers=headers)
    tracks_response.raise_for_status()
    tracks = tracks_response.json()["items"]

    # Denormalize: Add album and popularity info to each track
    for track in tracks:
        track['album'] = {
            'id': album_details.get('id'),
            'name': album_details.get('name'),
            'release_date': album_details.get('release_date'),
            'album_type': album_details.get('album_type')
        }
        # The `/albums/{id}/tracks` endpoint doesn't return track popularity.
        # We will fetch this in a later, more advanced version. For now, we can get album popularity.
        track['popularity'] = album_details.get('popularity', 0)
        
    return tracks