from email.mime import audio
import sys
import json
import time
import os
import pprint
from click import pass_context
import pandas as pd
import time
import logging
from sklearn.discriminant_analysis import unique_labels
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv


load_dotenv()
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET"),
        redirect_uri=os.getenv("REDIRECT_URI"),
        scope=os.getenv("SCOPE"),
    )
)
pp = pprint.PrettyPrinter(indent=5)


def print_playlists(path):
    filenames = os.listdir(path)
    for filename in sorted(filenames):
        if filename.startswith("mpd.slice.0-999") and filename.endswith(".json"):
            fullpath = os.sep.join((path, filename))
            f = open(fullpath)
            js = f.read()
            f.close()
            mpd_slice = json.loads(js)
            for playlist in mpd_slice["playlists"]:
                _ = json.dumps(playlist)
                pp.pprint(_)


def aggregate_tracks_from_playlists(path):
    filenames = os.listdir(path)
    aggregated_tracks = []
    for filename in sorted(filenames):
        if filename.startswith("mpd.slice.0-999") and filename.endswith(".json"):
            fullpath = os.sep.join((path, filename))
            f = open(fullpath)
            js = f.read()
            f.close()
            mpd_slice = json.loads(js)

            for playlist in mpd_slice["playlists"]:
                for tracks in playlist["tracks"]:
                    track_data = {
                        "track_name": tracks["track_name"],
                        "artist_name": tracks["artist_name"],
                        "track_uri": tracks["track_uri"],
                    }
                    aggregated_tracks.append(track_data)
                    # print(track_data)
    return aggregated_tracks


def find_and_remove_duplicates(raw_tracks):
    pp.pprint(f"raw tracks dict: {len(raw_tracks)}")
    df = pd.DataFrame(raw_tracks)
    df.drop_duplicates(subset=["track_uri"], keep="last", inplace=True)
    filtered_tracks = df.to_dict("records")
    pp.pprint(f"unique tracks after removing duplicates: {len(filtered_tracks)}")
    # pp.pprint(filtered_tracks)
    return filtered_tracks


def create_uri_list(tracks):
    track_uris = []
    for track in tracks:
        uri = track["track_uri"]
        # pp.pprint(uri)
        track_uris.append(uri)
    # print(len(track_uris), "length")
    return track_uris


def get_batched_audio_features(track_uris):
    audio_features = []
    raw_tracks = [track_uris[i : i + 100] for i in range(0, len(track_uris), 100)]
    for batch in raw_tracks:
        features = sp.audio_features(batch)
        audio_features.append(features)
        time.sleep(4.5)
        with open("audio_features.json", "w") as outfile:
            json.dump(features, outfile)
    outfile.close()


if __name__ == "__main__":
    raw_tracks = aggregate_tracks_from_playlists(
        "./spotify_million_playlist_dataset/data"
    )
    unique_tracks = find_and_remove_duplicates(raw_tracks)
    uri_list = create_uri_list(unique_tracks)
    get_batched_audio_features(uri_list)
