import os
import csv
import time
import requests
import urllib.parse
import re
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
        except:
            continue
    return datetime.min


def safe_request(url, params=None):
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except:
        return None


# =============================
# INSTAGRAM (NEW STRATEGY)
# =============================
def extract_instagram_from_text(text):
    if not text:
        return None

    matches = re.findall(r'https://www\.instagram\.com/[A-Za-z0-9_.]+', text)
    return matches[0] if matches else None


def find_instagram_profile(artist_name, lastfm_url=None, wiki_url=None):
    # 1. Try Wikipedia
    if wiki_url:
        try:
            html = requests.get(wiki_url).text
            ig = extract_instagram_from_text(html)
            if ig:
                return ig, 5
        except:
            pass

    # 2. Try Last.fm
    if lastfm_url:
        try:
            html = requests.get(lastfm_url).text
            ig = extract_instagram_from_text(html)
            if ig:
                return ig, 4
        except:
            pass

    return None, 0


# =============================
# SPOTIFY
# =============================
def get_spotify_client():
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def get_spotify_data(artist_name):
    sp = get_spotify_client()

    results = sp.search(q=artist_name, type="artist", limit=1)

    if not results["artists"]["items"]:
        return None

    artist = results["artists"]["items"][0]
    artist_id = artist["id"]

    data = {
        "name": artist["name"],
        "spotify_url": artist["external_urls"]["spotify"],
        "followers": artist.get("followers", {}).get("total", 0),
        "popularity": artist.get("popularity", 0),
        "genres": artist.get("genres", [])
    }

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

    if "tags" in artist:
        result["tags"] = [tag["name"] for tag in artist["tags"]["tag"]]

    if "url" in artist:
        result["lastfm_url"] = artist["url"]

    return result


# =============================
# WIKIPEDIA
# =============================
def get_wikipedia_link(artist_name):
    encoded = urllib.parse.quote(artist_name)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"

    data = safe_request(url)
    if not data:
        return None

    return data.get("content_urls", {}).get("desktop", {}).get("page")


# =============================
# CSV
# =============================
def flatten_artist_data(data):
    return {
        "name": data.get("name"),
        "spotify_url": data.get("spotify_url"),
        "followers": data.get("followers"),
        "popularity": data.get("popularity"),
        "genres": ", ".join(data.get("genres", [])),
        "tags": ", ".join(data.get("tags", [])),
        "lastfm_url": data.get("lastfm_url"),
        "wikipedia": data.get("wikipedia"),
        "instagram": data.get("instagram"),
        "instagram_score": data.get("instagram_score"),
    }


def save_to_csv(data, filename="artist_data.csv"):
    flat = flatten_artist_data(data)
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flat.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(flat)


# =============================
# MERGE
# =============================
def get_full_artist_profile(artist_name):
    spotify = get_spotify_data(artist_name)
    if not spotify:
        return None

    lastfm = get_lastfm_data(artist_name)
    wiki = get_wikipedia_link(artist_name)

    instagram, score = find_instagram_profile(
        artist_name,
        lastfm_url=lastfm.get("lastfm_url"),
        wiki_url=wiki
    )

    full = {**spotify, **lastfm}

    if wiki:
        full["wikipedia"] = wiki

    full["instagram"] = instagram
    full["instagram_score"] = score

    return full


# =============================
# BATCH
# =============================
def process_artists_from_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f if line.strip()]

    for artist in artists:
        print(f"Processing: {artist}")

        data = get_full_artist_profile(artist)

        if data:
            save_to_csv(data)

        time.sleep(0.5)


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    process_artists_from_file("input_artists.txt")