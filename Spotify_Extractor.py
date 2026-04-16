import os
import csv
import time
import json
import math
import re
import urllib.parse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher

import requests
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# =============================
# CONFIG
# =============================
LOG_FILE = "logfile.txt"
CSV_FILE = "artist_data.csv"

MAX_WORKERS = 5              # parallel workers for batch
BASE_SLEEP_SECONDS = 0.5     # base delay between external calls
MAX_RETRIES = 3              # for safe_request
BACKOFF_FACTOR = 1.5         # exponential backoff

STRUCTURED_JSON_LOG = False  # set True if you want JSON logs in logfile

# =============================
# LOGGING
# =============================
def log(message, level="INFO", extra=None):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if STRUCTURED_JSON_LOG:
        record = {
            "timestamp": timestamp,
            "level": level,
            "message": message,
            "extra": extra or {}
        }
        line = json.dumps(record, ensure_ascii=False)
    else:
        line = f"[{timestamp}] {message}"

    print(line)
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
# CACHES
# =============================
spotify_cache = {}
wikipedia_cache = {}
lastfm_cache = {}
instagram_cache = {}

# =============================
# HELPERS
# =============================
def similarity(a, b):
    a = (a or "").lower().strip()
    b = (b or "").lower().strip()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def safe_request(url, params=None, headers=None, expect_json=True, label=None):
    """
    Robust GET with retries, backoff, and proper User-Agent.
    """
    base_headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ArtistProfileCollector/1.0; "
            "+https://example.com/artist-scraper)"
        )
    }
    if headers:
        base_headers.update(headers)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, headers=base_headers, timeout=10)
            log(f"REQUEST OK → {url} ({response.status_code})", extra={"label": label})
            response.raise_for_status()
            return response.json() if expect_json else response.text
        except Exception as e:
            log(
                f"REQUEST FAILED (attempt {attempt}) → {url} | {e}",
                level="ERROR",
                extra={"label": label}
            )
            if attempt == MAX_RETRIES:
                return None
            sleep_time = BASE_SLEEP_SECONDS * (BACKOFF_FACTOR ** (attempt - 1))
            time.sleep(sleep_time)


def extract_instagram_links(html):
    """
    Extract Instagram profile URLs from HTML and filter out known non-artist accounts.
    """
    if not html:
        return []

    raw_links = re.findall(r'https://www\.instagram\.com/[A-Za-z0-9_.]+/?', html)
    blacklist_substrings = ["last_fm", "last.fm", "instagram.com/instagram"]
    cleaned = []
    for link in raw_links:
        lower = link.lower()
        if any(bad in lower for bad in blacklist_substrings):
            continue
        if link not in cleaned:
            cleaned.append(link)
    return cleaned


def extract_social_links_from_html(html):
    """
    Extract common social links from Wikipedia or other HTML.
    """
    if not html:
        return {}

    socials = {
        "instagram": [],
        "facebook": [],
        "youtube": [],
        "tiktok": [],
        "website": []
    }

    socials["instagram"] = extract_instagram_links(html)

    fb_links = re.findall(r'https?://www\.facebook\.com/[A-Za-z0-9_.\-]+/?', html)
    yt_links = re.findall(r'https?://www\.youtube\.com/[A-Za-z0-9_/\-]+', html)
    tt_links = re.findall(r'https?://www\.tiktok\.com/@[A-Za-z0-9_.\-]+', html)
    web_links = re.findall(r'https?://[A-Za-z0-9_\-\.]+\.[A-Za-z]{2,}[^"\'\s<]*', html)

    socials["facebook"] = list(dict.fromkeys(fb_links))
    socials["youtube"] = list(dict.fromkeys(yt_links))
    socials["tiktok"] = list(dict.fromkeys(tt_links))

    website_clean = []
    for link in web_links:
        lower = link.lower()
        if any(s in lower for s in ["facebook.com", "instagram.com", "youtube.com", "tiktok.com", "twitter.com", "x.com"]):
            continue
        if link not in website_clean:
            website_clean.append(link)
    socials["website"] = website_clean

    return socials


def extract_birth_info_from_wiki_html(html):
    """
    Very rough extraction of birth date / origin from Wikipedia HTML.
    """
    if not html:
        return {}

    info = {}

    birth_match = re.search(r'Born</th>\s*<td[^>]*>(.*?)</td>', html, re.IGNORECASE | re.DOTALL)
    if birth_match:
        text = re.sub(r'<.*?>', ' ', birth_match.group(1))
        info["birth_raw"] = " ".join(text.split())

    origin_match = re.search(r'Origin</th>\s*<td[^>]*>(.*?)</td>', html, re.IGNORECASE | re.DOTALL)
    if origin_match:
        text = re.sub(r'<.*?>', ' ', origin_match.group(1))
        info["origin_raw"] = " ".join(text.split())

    return info


# =============================
# MONTHLY LISTENERS
# =============================
def get_monthly_listeners(artist_id):
    url = f"https://open.spotify.com/artist/{artist_id}"

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        html = safe_request(url, headers=headers, expect_json=False, label="spotify_monthly")
        if not html:
            return None

        match = re.search(r'([0-9.,]+)\s*monthly listeners', html, re.IGNORECASE)
        if match:
            listeners = int(match.group(1).replace(",", "").replace(".", ""))
            log(f"Monthly listeners FOUND: {listeners}")
            return listeners
        else:
            log("Monthly listeners NOT found")
    except Exception as e:
        log(f"Monthly listener error: {e}", level="ERROR")

    return None


# =============================
# INSTAGRAM
# =============================
def validate_instagram_profile(url, artist_name):
    """
    Validate guessed Instagram profile by checking HTML for basic signals.
    """
    if not url:
        return None, 0, None

    if url in instagram_cache:
        return instagram_cache[url]

    try:
        html = safe_request(url, expect_json=False, label="instagram_profile")
        if not html:
            instagram_cache[url] = (None, 0, None)
            return None, 0, None

        followers = None
        follower_match = re.search(r'"edge_followed_by":{"count":(\d+)}', html)
        if follower_match:
            followers = int(follower_match.group(1))

        has_posts = '"edge_owner_to_timeline_media":{"count":' in html
        is_private = '"is_private":true' in html
        name_similarity = similarity(artist_name, url.split("instagram.com/")[-1].strip("/"))

        score = 0
        if followers:
            score += min(3, math.log10(followers + 1))
        if has_posts:
            score += 1
        if not is_private:
            score += 1
        score += name_similarity * 3

        instagram_cache[url] = (url, score, followers)
        return url, score, followers

    except Exception as e:
        log(f"Instagram validation error ({url}): {e}", level="ERROR")
        instagram_cache[url] = (None, 0, None)
        return None, 0, None


def find_instagram_profile(artist_name, lastfm_url=None, wiki_url=None):
    """
    Multi-source Instagram discovery:
    1) Wikipedia HTML
    2) Last.fm HTML
    3) Heuristic guesses with validation
    """
    # Wikipedia
    if wiki_url:
        try:
            html = safe_request(wiki_url, expect_json=False, label="wiki_html")
            ig_links = extract_instagram_links(html)
            for link in ig_links:
                url, score, followers = validate_instagram_profile(link, artist_name)
                if url and score > 2:
                    log(f"Instagram found via Wikipedia: {url}")
                    return url, score, followers
        except Exception as e:
            log(f"Wikipedia IG error: {e}", level="ERROR")

    # Last.fm
    if lastfm_url:
        try:
            html = safe_request(lastfm_url, expect_json=False, label="lastfm_html")
            ig_links = extract_instagram_links(html)
            for link in ig_links:
                url, score, followers = validate_instagram_profile(link, artist_name)
                if url and score > 2:
                    log(f"Instagram found via Last.fm: {url}")
                    return url, score, followers
        except Exception as e:
            log(f"Last.fm IG error: {e}", level="ERROR")

    # Heuristic guesses
    clean = re.sub(r'[^a-z0-9]', '', artist_name.lower())

    guesses = [
        clean,
        clean + "official",
        clean + "music",
        clean + "artist",
        clean + "dj",
        clean + "producer",
    ]

    best_url = None
    best_score = 0
    best_followers = None

    for username in guesses:
        url = f"https://www.instagram.com/{username}/"
        try:
            candidate_url, score, followers = validate_instagram_profile(url, artist_name)
            if candidate_url and score > best_score:
                best_url, best_score, best_followers = candidate_url, score, followers
        except Exception as e:
            log(f"Instagram guess failed ({url}): {e}", level="ERROR")

    if best_url:
        log(f"Instagram guessed: {best_url} (score={best_score:.2f})")
        return best_url, best_score, best_followers

    log("Instagram NOT found")
    return None, 0, None


# =============================
# SPOTIFY
# =============================
_spotify_client = None

def get_spotify_client():
    global _spotify_client
    if _spotify_client is None:
        auth_manager = SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        )
        _spotify_client = spotipy.Spotify(auth_manager=auth_manager)
    return _spotify_client


def _select_best_spotify_artist(items, artist_name):
    """
    Prefer exact name match, then fuzzy similarity, then popularity.
    """
    if not items:
        return None

    target = artist_name.strip().lower()

    def score(a):
        name = a.get("name", "").strip()
        name_lower = name.lower()
        exact = (name_lower == target)
        sim = similarity(name_lower, target)
        popularity = a.get("popularity", 0) or 0
        return (
            1 if exact else 0,
            sim,
            popularity
        )

    best = max(items, key=score)
    return best


def get_spotify_data(artist_name):
    if artist_name in spotify_cache:
        return spotify_cache[artist_name]

    sp = get_spotify_client()

    try:
        results = sp.search(q=f"artist:{artist_name}", type="artist", limit=10)
        items = results.get("artists", {}).get("items", [])

        if not items:
            results = sp.search(q=artist_name, type="artist", limit=10)
            items = results.get("artists", {}).get("items", [])

        if not items:
            log("Spotify: artist not found")
            spotify_cache[artist_name] = None
            return None

        artist = _select_best_spotify_artist(items, artist_name)
        if not artist:
            log("Spotify: no suitable artist match")
            spotify_cache[artist_name] = None
            return None

        artist_id = artist["id"]
        log(f"Spotify artist found: {artist['name']}")

        monthly_listeners = get_monthly_listeners(artist_id)

        images = artist.get("images", [])
        image_url = images[0]["url"] if images else None

        data = {
            "name": artist["name"],
            "spotify_url": artist["external_urls"]["spotify"],
            "followers": artist.get("followers", {}).get("total", 0),
            "popularity": artist.get("popularity", 0),
            "genres": artist.get("genres", []),
            "monthly_listeners": monthly_listeners,
            "spotify_image": image_url,
            # release scraping disabled (Option B)
            "spotify_earliest_release": None,
            "spotify_latest_release": None,
            "spotify_career_span_years": None,
        }

        spotify_cache[artist_name] = data
        return data

    except Exception as e:
        log(f"Spotify error: {e}", level="ERROR")
        spotify_cache[artist_name] = None
        return None


# =============================
# LAST.FM
# =============================
def get_lastfm_data(artist_name):
    if artist_name in lastfm_cache:
        return lastfm_cache[artist_name]

    if not LASTFM_API_KEY:
        log("No Last.fm API key")
        lastfm_cache[artist_name] = {}
        return {}

    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "artist.getinfo",
        "artist": artist_name,
        "api_key": LASTFM_API_KEY,
        "format": "json"
    }

    data = safe_request(url, params, label="lastfm")
    if not data or "artist" not in data:
        log("Last.fm: no data")
        lastfm_cache[artist_name] = {}
        return {}

    artist = data["artist"]

    result = {}

    if "tags" in artist:
        result["tags"] = [tag["name"] for tag in artist["tags"]["tag"]]

    if "url" in artist:
        result["lastfm_url"] = artist["url"]

    bio = artist.get("bio", {}).get("summary") or ""
    bio_clean = re.sub(r'<.*?>', '', bio).strip()
    if bio_clean:
        result["lastfm_bio"] = bio_clean[:300]

    similar = artist.get("similar", {}).get("artist", [])
    similar_names = [a.get("name") for a in similar if a.get("name")]
    if similar_names:
        result["lastfm_similar_artists"] = similar_names

    log("Last.fm data retrieved")
    lastfm_cache[artist_name] = result
    return result


# =============================
# WIKIPEDIA
# =============================
def _wikipedia_search_title(artist_name):
    search_url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "list": "search",
        "srsearch": artist_name,
        "format": "json",
        "srlimit": 5
    }
    data = safe_request(search_url, params, label="wiki_search")
    if not data:
        return None

    results = data.get("query", {}).get("search", [])
    if not results:
        return None

    best = max(results, key=lambda r: similarity(r.get("title", ""), artist_name))
    return best.get("title")


def get_wikipedia_link(artist_name):
    if artist_name in wikipedia_cache:
        return wikipedia_cache[artist_name]

    encoded = urllib.parse.quote(artist_name)
    url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"

    data = safe_request(url, label="wiki_summary")
    if not data:
        log("Wikipedia not found")
        wikipedia_cache[artist_name] = None
        return None

    if "type" in data and data["type"] == "https://mediawiki.org/wiki/HyperSwitch/errors/not_found":
        title = _wikipedia_search_title(artist_name)
        if not title:
            log("Wikipedia not found (after search)")
            wikipedia_cache[artist_name] = None
            return None
        encoded_title = urllib.parse.quote(title)
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded_title}"
        data = safe_request(url, label="wiki_summary_fallback")
        if not data:
            log("Wikipedia not found (fallback summary)")
            wikipedia_cache[artist_name] = None
            return None

    log("Wikipedia found")
    page_url = data.get("content_urls", {}).get("desktop", {}).get("page")
    wikipedia_cache[artist_name] = page_url
    return page_url


def get_wikipedia_extended_data(page_url):
    if not page_url:
        return {}

    try:
        html = safe_request(page_url, expect_json=False, label="wiki_html_extended")
        socials = extract_social_links_from_html(html)
        birth_info = extract_birth_info_from_wiki_html(html)
        result = {}
        result.update(socials)
        result.update(birth_info)
        return result
    except Exception as e:
        log(f"Wikipedia extended data error: {e}", level="ERROR")
        return {}


# =============================
# CSV
# =============================
def flatten_artist_data(data):
    return {
        "timestamp": data.get("timestamp"),
        "name": data.get("name"),
        "spotify_url": data.get("spotify_url"),
        "spotify_image": data.get("spotify_image"),
        "followers": data.get("followers"),
        "popularity": data.get("popularity"),
        "monthly_listeners": data.get("monthly_listeners"),
        "spotify_earliest_release": data.get("spotify_earliest_release"),
        "spotify_latest_release": data.get("spotify_latest_release"),
        "spotify_career_span_years": data.get("spotify_career_span_years"),
        "genres": ", ".join(data.get("genres", [])),
        "tags": ", ".join(data.get("tags", [])),
        "lastfm_url": data.get("lastfm_url"),
        "lastfm_bio": data.get("lastfm_bio"),
        "lastfm_similar_artists": ", ".join(data.get("lastfm_similar_artists", [])),
        "wikipedia": data.get("wikipedia"),
        "instagram": data.get("instagram"),
        "instagram_score": data.get("instagram_score"),
        "instagram_followers": data.get("instagram_followers"),
        "wiki_instagram": ", ".join(data.get("wiki_instagram", [])),
        "wiki_facebook": ", ".join(data.get("wiki_facebook", [])),
        "wiki_youtube": ", ".join(data.get("wiki_youtube", [])),
        "wiki_tiktok": ", ".join(data.get("wiki_tiktok", [])),
        "wiki_website": ", ".join(data.get("wiki_website", [])),
        "wiki_birth_raw": data.get("wiki_birth_raw"),
        "wiki_origin_raw": data.get("wiki_origin_raw"),
        "source_quality_score": data.get("source_quality_score"),
        "data_completeness": data.get("data_completeness"),
    }


def save_to_csv(data, filename=CSV_FILE):
    flat = flatten_artist_data(data)
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flat.keys())

        if not file_exists:
            writer.writeheader()

        writer.writerow(flat)

    log("Saved to CSV")


# =============================
# QUALITY & COMPLETENESS
# =============================
def compute_source_quality_score(profile):
    score = 0.0

    if profile.get("spotify_url"):
        score += 2
    if profile.get("monthly_listeners") is not None:
        score += 1

    ig_score = profile.get("instagram_score") or 0
    score += min(3, ig_score / 2.0)

    if profile.get("wikipedia"):
        score += 2

    if profile.get("lastfm_url"):
        score += 1
    if profile.get("lastfm_bio"):
        score += 1

    return round(score, 2)


def compute_data_completeness(profile):
    fields = [
        "spotify_url",
        "followers",
        "popularity",
        "monthly_listeners",
        "genres",
        "tags",
        "lastfm_url",
        "lastfm_bio",
        "wikipedia",
        "instagram",
    ]
    present = 0
    for f in fields:
        v = profile.get(f)
        if v not in (None, "", []):
            present += 1
    completeness = present / len(fields) if fields else 0
    return round(completeness * 100, 1)


# =============================
# MERGE
# =============================
def get_full_artist_profile(artist_name):
    start_time = time.time()
    log(f"=== Processing: {artist_name} ===")

    spotify = get_spotify_data(artist_name)
    if not spotify:
        log("No Spotify data → skipping artist", level="WARNING")
        return None

    lastfm = get_lastfm_data(artist_name)
    wiki_url = get_wikipedia_link(artist_name)
    wiki_extended = get_wikipedia_extended_data(wiki_url) if wiki_url else {}

    instagram, ig_score, ig_followers = find_instagram_profile(
        artist_name,
        lastfm_url=lastfm.get("lastfm_url"),
        wiki_url=wiki_url
    )

    full = {}
    full.update(spotify)
    full.update(lastfm)

    if wiki_url:
        full["wikipedia"] = wiki_url

    full["wiki_instagram"] = wiki_extended.get("instagram", [])
    full["wiki_facebook"] = wiki_extended.get("facebook", [])
    full["wiki_youtube"] = wiki_extended.get("youtube", [])
    full["wiki_tiktok"] = wiki_extended.get("tiktok", [])
    full["wiki_website"] = wiki_extended.get("website", [])
    full["wiki_birth_raw"] = wiki_extended.get("birth_raw")
    full["wiki_origin_raw"] = wiki_extended.get("origin_raw")

    full["instagram"] = instagram
    full["instagram_score"] = ig_score
    full["instagram_followers"] = ig_followers

    full["timestamp"] = datetime.now().isoformat()

    full["source_quality_score"] = compute_source_quality_score(full)
    full["data_completeness"] = compute_data_completeness(full)

    elapsed = time.time() - start_time
    log(
        f"Finished {artist_name} in {elapsed:.2f}s "
        f"(quality={full['source_quality_score']}, completeness={full['data_completeness']}%)"
    )

    return full


# =============================
# BATCH
# =============================
def _process_single_artist(artist):
    try:
        profile = get_full_artist_profile(artist)
        if profile:
            save_to_csv(profile)
        else:
            log(f"No data found for {artist}", level="WARNING")
    except Exception as e:
        log(f"Unhandled error for {artist}: {e}", level="ERROR")


def process_artists_from_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        artists = [line.strip() for line in f if line.strip()]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_process_single_artist, artist): artist for artist in artists}
        for future in as_completed(futures):
            artist = futures[future]
            try:
                future.result()
            except Exception as e:
                log(f"Error in future for {artist}: {e}", level="ERROR")


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    process_artists_from_file("input_artists.txt")
