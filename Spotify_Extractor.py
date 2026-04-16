import os
import requests
import urllib.parse
from datetime import datetime
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# =============================
# LOAD ENV
# =============================
load_dotenv()

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")

if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
    raise ValueError("Spotify credentials not set in environment variables.")

# =============================
# HELPERS
# =============================
def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return datetime.min


def safe_request(url, params=None):
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


# =============================
# SPOTIFY
# =============================
def get_spotify_client():
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def find_best_artist_match(sp, artist_name):
    try:
        results = sp.search(q=artist_name, type="artist", limit=5)
    except Exception:
        return None

    items = results.get("artists", {}).get("items", [])
    if not items:
        return None

    for artist in items:
        if artist.get("name", "").lower() == artist_name.lower():
            return artist

    return items[0]


def get_all_releases(sp, artist_id):
    try:
        albums = sp.artist_albums(
            artist_id,
            limit=50,
            include_groups="album,single"
        )
    except Exception:
        return []

    all_items = albums.get("items", [])

    while albums.get("next"):
        try:
            albums = sp.next(albums)
            all_items.extend(albums.get("items", []))
        except Exception:
            break

    # Deduplicate
    seen = set()
    unique = []

    for album in all_items:
        name = album.get("name")
        if name and name not in seen:
            seen.add(name)
            unique.append(album)

    return unique


def get_spotify_data(artist_name):
    sp = get_spotify_client()

    artist = find_best_artist_match(sp, artist_name)
    if not artist:
        return None

    artist_id = artist.get("id")

    # 🔥 Get full artist object
    try:
        artist = sp.artist(artist_id)
    except Exception:
        return None

    data = {
        "name": artist.get("name"),
        "spotify_url": artist.get("external_urls", {}).get("spotify"),
        "followers": artist.get("followers", {}).get("total", 0),
        "popularity": artist.get("popularity", 0),
        "genres": artist.get("genres", []),
        "image": artist.get("images", [{}])[0].get("url") if artist.get("images") else None
    }

    # Top tracks with fallback
    try:
        top_tracks = sp.artist_top_tracks(artist_id, country="DE")

        if not top_tracks.get("tracks"):
            top_tracks = sp.artist_top_tracks(artist_id, country="US")

        data["top_tracks"] = [
            {
                "name": t.get("name"),
                "url": t.get("external_urls", {}).get("spotify")
            }
            for t in top_tracks.get("tracks", [])[:5]
        ]
    except Exception:
        data["top_tracks"] = []

    # Releases
    releases = get_all_releases(sp, artist_id)

    all_releases = [
        {
            "name": album.get("name"),
            "release_date": album.get("release_date"),
            "url": album.get("external_urls", {}).get("spotify")
        }
        for album in releases
    ]

    all_releases_sorted = sorted(
        all_releases,
        key=lambda x: parse_date(x.get("release_date", "")),
        reverse=True
    )

    if all_releases_sorted:
        data["latest_release"] = all_releases_sorted[0]

    return data


# =============================
# LAST.FM
# =============================
def get_lastfm_data(artist_name):
    if not LASTFM_API_KEY:
        return {}

    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.getinfo",
        "artist": artist_name,
        "api_key": LASTFM_API_KEY,
        "format": "json"
    }

    data = safe_request(url, params)
    if not data or "artist" not in data:
        return {}

    artist = data["artist"]

    result = {}

    if "bio" in artist and "summary" in artist["bio"]:
        result["bio"] = artist["bio"]["summary"]

    if "tags" in artist:
        result["tags"] = [tag["name"] for tag in artist["tags"]["tag"]]

    if "url" in artist:
        result["lastfm_url"] = artist["url"]

    return result


# =============================
# WIKIPEDIA
# =============================
def get_wikipedia_link(artist_name):
    encoded_name = urllib.parse.quote(artist_name)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded_name}"

    data = safe_request(url)
    if not data:
        return None

    return data.get("content_urls", {}).get("desktop", {}).get("page")


# =============================
# MERGE
# =============================
def get_full_artist_profile(artist_name):
    spotify = get_spotify_data(artist_name)
    if not spotify:
        return None

    lastfm = get_lastfm_data(artist_name)
    wiki = get_wikipedia_link(artist_name)

    full_data = {**spotify, **lastfm}

    if wiki:
        full_data["wikipedia"] = wiki

    return full_data


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    artist_name = input("Artist Name: ").strip()

    data = get_full_artist_profile(artist_name)

    if not data:
        print("Artist nicht gefunden.")
    else:
        print("\n===== ARTIST PROFILE =====\n")
        for key, value in data.items():
            print(f"{key}: {value}")