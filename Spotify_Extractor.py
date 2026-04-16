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
# LOGGING
# =============================
LOG_FILE = "logfile.txt"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)  # keep console output
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


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
        except Exception:
            continue
    return datetime.min


def safe_request(url, params=None):
    """
    Generic JSON GET with proper User-Agent so Wikipedia / APIs don't 403.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ArtistProfileCollector/1.0; "
            "+https://example.com/artist-scraper)"
        )
    }
    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        log(f"REQUEST OK → {url} ({response.status_code})")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log(f"REQUEST FAILED → {url} | {e}")
        return None


def extract_instagram_links(html):
    """
    Extract Instagram profile URLs from HTML and filter out known non-artist accounts
    like Last.fm's own profile.
    """
    raw_links = re.findall(r'https://www\.instagram\.com/[A-Za-z0-9_.]+', html)
    blacklist_substrings = ["last_fm", "last.fm", "instagram.com/instagram"]
    cleaned = []
    for link in raw_links:
        lower = link.lower()
        if any(bad in lower for bad in blacklist_substrings):
            continue
        cleaned.append(link)
    return cleaned


# =============================
# MONTHLY LISTENERS
# =============================
def get_monthly_listeners(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text

        match = re.search(r'([0-9.,]+)\s*monthly listeners', html, re.IGNORECASE)

        if match:
            listeners = int(match.group(1).replace(",", "").replace(".", ""))
            log(f"Monthly listeners FOUND: {listeners}")
            return listeners
        else:
            log("Monthly listeners NOT found")

    except Exception as e:
        log(f"Monthly listener error: {e}")

    return None


# =============================
# INSTAGRAM
# =============================
def find_instagram_profile(artist_name, lastfm_url=None, wiki_url=None):

    # Wikipedia
    if wiki_url:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            html = requests.get(wiki_url, headers=headers, timeout=10).text
            ig_links = extract_instagram_links(html)
            if ig_links:
                log(f"Instagram found via Wikipedia: {ig_links[0]}")
                return ig_links[0], 5
        except Exception as e:
            log(f"Wikipedia IG error: {e}")

    # Last.fm
    if lastfm_url:
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            html = requests.get(lastfm_url, headers=headers, timeout=10).text
            ig_links = extract_instagram_links(html)
            if ig_links:
                log(f"Instagram found via Last.fm: {ig_links[0]}")
                return ig_links[0], 4
        except Exception as e:
            log(f"Last.fm IG error: {e}")

    # Heuristic
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
                log(f"Instagram guessed: {url}")
                return url, 3
        except Exception as e:
            log(f"Instagram guess failed ({url}): {e}")

    log("Instagram NOT found")
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


def _select_best_spotify_artist(items, artist_name):
    """
    Prefer exact name match (case-insensitive), then highest popularity.
    """
    if not items:
        return None

    target = artist_name.strip().lower()

    def score(a):
        name = a.get("name", "").strip().lower()
        exact = (name == target)
        popularity = a.get("popularity", 0) or 0
        # exact match first, then popularity
        return (1 if exact else 0, popularity)

    best = max(items, key=score)
    return best


def get_spotify_data(artist_name):
    sp = get_spotify_client()

    try:
        # Use artist: query and allow multiple results, then pick best match
        results = sp.search(q=f"artist:{artist_name}", type="artist", limit=5)

        items = results.get("artists", {}).get("items", [])
        if not items:
            log("Spotify: artist not found")
            return None

        artist = _select_best_spotify_artist(items, artist_name)
        if not artist:
            log("Spotify: no suitable artist match")
            return None

        artist_id = artist["id"]

        log(f"Spotify artist found: {artist['name']}")

        data = {
            "name": artist["name"],
            "spotify_url": artist["external_urls"]["spotify"],
            "followers": artist.get("followers", {}).get("total", 0),
            "popularity": artist.get("popularity", 0),
            "genres": artist.get("genres", []),
            "monthly_listeners": get_monthly_listeners(artist_id)
        }

        return data

    except Exception as e:
        log(f"Spotify error: {e}")
        return None


# =============================
# LAST.FM
# =============================
def get_lastfm_data(artist_name):
    if not LASTFM_API_KEY:
        log("No Last.fm API key")
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
        log("Last.fm: no data")
        return {}

    artist = data["artist"]

    result = {}

    if "tags" in artist:
        result["tags"] = [tag["name"] for tag in artist["tags"]["tag"]]

    if "url" in artist:
        result["lastfm_url"] = artist["url"]

    log("Last.fm data retrieved")

    return result


# =============================
# WIKIPEDIA
# =============================
def get_wikipedia_link(artist_name):
    encoded = urllib.parse.quote(artist_name)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"

    data = safe_request(url)
    if not data:
        log("Wikipedia not found")
        return None

    log("Wikipedia found")
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

    log("Saved to CSV")


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
        log(f"\n=== Processing: {artist} ===")

        data = get_full_artist_profile(artist)

        if data:
            save_to_csv(data)
        else:
            log("No data found")

        time.sleep(1)


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    process_artists_from_file("input_artists.txt")
