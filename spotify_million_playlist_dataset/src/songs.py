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
    logging.info(f"Finding and removing duplicate tracks.")
    logging.info(f"Raw tracks dict: {len(raw_tracks)}")
    df = pd.DataFrame(raw_tracks)
    df.drop_duplicates(subset=["track_uri"], keep="last", inplace=True)
    filtered_tracks = df.to_dict("records")
    logging.info(f"Unique tracks after removing duplicates: {len(filtered_tracks)}")
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


""" The function below is intended to replace this list comprehension: 
raw_tracks = [track_uris[i : i + 100] for i in range(0, len(track_uris), 100)] 
"""


def process_raw_tracks(track_uris):
    logging.info(f"Beginning processing batches of raw tracks.")
    batch_size = 100
    raw_tracks = []
    for i in range(0, len(track_uris), batch_size):
        logging.info(f"Processing batch from index {i} to {i + batch_size}")
        batch = track_uris[i : i + batch_size]
        raw_tracks.append(batch)
    return raw_tracks


def get_batched_audio_features(track_uris):
    audio_features = []
    raw_tracks = process_raw_tracks(track_uris)
    outfile = open("audio_features.json", "w")
    for index, batch in enumerate(raw_tracks):
        logging.info(f"Getting features for batch: {index}")
        features = sp.audio_features(batch)
        audio_features.append(features)
        time.sleep(4.5)
        json.dump(features, outfile)
    outfile.close()


def setup_logger():
    logging.basicConfig(
        filename="batched_audio_features_log.log",
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(levelname)s:%(message)s - %(asctime)s ",
    )


# add try exceptions for stack tracing error logging too
# https://realpython.com/python-logging/#:~:text=Handler%20%3A%20Handlers%20send%20the%20LogRecord,stdout%20or%20a%20disk%20file
if __name__ == "__main__":
    load_dotenv()
    setup_logger()
    pp = pprint.PrettyPrinter(indent=5)
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=os.getenv("CLIENT_ID"),
            client_secret=os.getenv("CLIENT_SECRET"),
            redirect_uri=os.getenv("REDIRECT_URI"),
            scope=os.getenv("SCOPE"),
        )
    )
    logging.info(f"Program has started.")
    raw_tracks = aggregate_tracks_from_playlists(
        "./spotify_million_playlist_dataset/data"
    )
    unique_tracks = find_and_remove_duplicates(raw_tracks)
    uri_list = create_uri_list(unique_tracks)
    get_batched_audio_features(uri_list)
