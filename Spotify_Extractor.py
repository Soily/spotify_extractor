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


def get_monthly_listeners(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text

        match = re.search(r'([0-9.,]+)\smonthly listeners', html)
        if match:
            return int(match.group(1).replace(",", "").replace(".", ""))
    except Exception:
        pass

    return None


# =============================
# INSTAGRAM (SCORING)
# =============================
def find_instagram_profile(artist_name):
    query = f'site:instagram.com "{artist_name}"'
    url = f"https://www.google.com/search?q={urllib.parse.quote(query)}"

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        html = response.text

        matches = re.findall(r"https://www\.instagram\.com/[A-Za-z0-9_.]+", html)

        best_match = None
        best_score = 0

        clean_name = re.sub(r'[^a-z0-9]', '', artist_name.lower())

        for link in matches:
            username = link.split("/")[-1].lower()
            clean_user = re.sub(r'[^a-z0-9]', '', username)

            score = 0

            # Strong match
            if clean_name == clean_user:
                score += 5
            elif clean_name in clean_user:
                score += 3

            # Weak match
            if artist_name.lower() in username:
                score += 2

            if score > best_score:
                best_score = score
                best_match = link

        return best_match, best_score

    except Exception:
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
        "image": artist.get("images", [{}])[0].get("url") if artist.get("images") else None,
        "monthly_listeners": get_monthly_listeners(artist_id)
    }

    # Top tracks
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
# CSV EXPORT
# =============================
def flatten_artist_data(data):
    flat = {
        "name": data.get("name"),
        "spotify_url": data.get("spotify_url"),
        "followers": data.get("followers"),
        "popularity": data.get("popularity"),
        "monthly_listeners": data.get("monthly_listeners"),
        "genres": ", ".join(data.get("genres", [])),
        "image": data.get("image"),
        "lastfm_url": data.get("lastfm_url"),
        "wikipedia": data.get("wikipedia"),
        "tags": ", ".join(data.get("tags", [])),
        "instagram": data.get("instagram"),
        "instagram_score": data.get("instagram_score")
    }

    top_tracks = data.get("top_tracks", [])
    flat["top_tracks"] = ", ".join([t.get("name", "") for t in top_tracks])

    latest = data.get("latest_release", {})
    flat["latest_release_name"] = latest.get("name")
    flat["latest_release_date"] = latest.get("release_date")
    flat["latest_release_url"] = latest.get("url")

    return flat


def save_to_csv(data, filename="artist_data.csv"):
    flat_data = flatten_artist_data(data)

    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=flat_data.keys(),
            quoting=csv.QUOTE_ALL  # FIXED CSV ISSUE
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(flat_data)


# =============================
# MERGE
# =============================
def get_full_artist_profile(artist_name):
    spotify = get_spotify_data(artist_name)
    if not spotify:
        return None

    lastfm = get_lastfm_data(artist_name)
    wiki = get_wikipedia_link(artist_name)

    instagram, score = find_instagram_profile(artist_name)

    full_data = {**spotify, **lastfm}

    if wiki:
        full_data["wikipedia"] = wiki

    if instagram:
        full_data["instagram"] = instagram
        full_data["instagram_score"] = score

    return full_data


# =============================
# BATCH PROCESSING
# =============================
def process_artists_from_file(filename):
    if not os.path.isfile(filename):
        print(f"❌ File not found: {filename}")
        return

    with open(filename, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f if line.strip()]

    print(f"\n📂 Found {len(artists)} artists\n")

    for i, artist_name in enumerate(artists, start=1):
        print(f"\n[{i}/{len(artists)}] Processing: {artist_name}")

        try:
            data = get_full_artist_profile(artist_name)

            if not data:
                print("❌ Not found, skipping.")
                continue

            save_to_csv(data)
            print(f"✅ Saved (IG score: {data.get('instagram_score', 0)})")

            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Error: {e}")
            continue

    print("\n🎉 Batch processing complete!")


# =============================
# MAIN (AUTO-BATCH)
# =============================
if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(BASE_DIR, "input_artists.txt")

    if not os.path.isfile(filename):
        print(f"❌ File not found: {filename}")
        exit(1)

    print(f"📂 Using artist list: {filename}")
    process_artists_from_file(filename)