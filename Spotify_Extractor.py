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
# MONTHLY LISTENERS (FIXED)
# =============================
def get_monthly_listeners(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, headers=headers, timeout=10)
        html = response.text

        # More robust regex
        match = re.search(r'([0-9.,]+)\s*monthly listeners', html, re.IGNORECASE)

        if match:
            listeners = int(match.group(1).replace(",", "").replace(".", ""))
            print(f"✔ Monthly listeners: {listeners}")
            return listeners
        else:
            print("⚠ Monthly listeners not found")

    except Exception as e:
        print(f"Monthly listener error: {e}")

    return None


# =============================
# INSTAGRAM (HYBRID)
# =============================
def find_instagram_profile(artist_name, lastfm_url=None, wiki_url=None):
    # Wikipedia
    if wiki_url:
        try:
            html = requests.get(wiki_url, timeout=10).text
            ig = re.findall(r'https://www\.instagram\.com/[A-Za-z0-9_.]+', html)
            if ig:
                return ig[0], 5
        except:
            pass

    # Last.fm
    if lastfm_url:
        try:
            html = requests.get(lastfm_url, timeout=10).text
            ig = re.findall(r'https://www\.instagram\.com/[A-Za-z0-9_.]+', html)
            if ig:
                return ig[0], 4
        except:
            pass

    # Heuristic guesses
    clean = re.sub(r'[^a-z0-9]', '', artist_name.lower())

    guesses = [
        clean,
        clean + "official",
        clean + "music"
    ]

    for username in guesses:
        url = f"https://www.instagram.com/{username}/"
        try:
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                return url, 3
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
        "genres": artist.get("genres", []),
        "monthly_listeners": get_monthly_listeners(artist_id)
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
        "monthly_listeners": data.get("monthly_listeners"),
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
        print(f"\nProcessing: {artist}")

        data = get_full_artist_profile(artist)

        if data:
            save_to_csv(data)

        time.sleep(1)


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    process_artists_from_file("input_artists.txt")