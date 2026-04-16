import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
    client_id="ee04810070324f65bdbe8b8a222680b7",
    client_secret="361a7010254e41caa153800321b58e32"
))

print(sp.search(q="Drake", type="artist"))