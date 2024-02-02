import json
import time
import os
import pandas as pd
import time
import logging
import threading
from globals import initialize_spotipy


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
    return aggregated_tracks


def find_and_remove_duplicate_tracks(raw_tracks):
    logging.info(f"Finding and removing duplicate tracks.")
    print(f"Finding and removing duplicate tracks.")
    logging.info(f"Raw tracks before filtering: {len(raw_tracks)}")
    print(f"Raw tracks before filtering: {len(raw_tracks)}")
    df = pd.DataFrame(raw_tracks)
    df.drop_duplicates(subset=["track_uri"], keep="last", inplace=True)
    filtered_tracks = df.to_dict("records")
    logging.info(f"Unique tracks after removing duplicates: {len(filtered_tracks)}")
    print(f"Unique tracks after removing duplicates: {len(filtered_tracks)}")
    return filtered_tracks


def create_uri_list(tracks):
    track_uris = []
    for track in tracks:
        uri = track["track_uri"]
        track_uris.append(uri)
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


def prepare_uris_for_dataset():
    raw_tracks = aggregate_tracks_from_playlists(
        "./spotify_million_playlist_dataset/data"
    )

    unique_tracks = find_and_remove_duplicate_tracks(raw_tracks)
    print(f"Duplicates removed.")

    uri_list = create_uri_list(unique_tracks)
    return uri_list


def get_batched_audio_features_from_spotify_api(track_uris):
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


def get_audio_analysis_from_spotify_api(uri_list):
    audio_analysis_list = []
    outfile = open("audio_analysis_dataset.json", "w")
    for uri in uri_list:
        song_analysis = sp.audio_analysis(uri)
        time.sleep(4.5)
        audio_analysis_list.append(song_analysis)
        json.dump(song_analysis, outfile)
    outfile.close()


def thread_audio_features_from_spotify_api(uri_list):
    logging.info(f"Audio features thread starting...")
    get_batched_audio_features_from_spotify_api(uri_list)
    logging.info(f"Audio features thread has finished.")
    print(f"Main    : Audio features thread has finished.")


def thread_audio_analysis(uri_list):
    logging.info(f"Audio analysis thread starting...")
    get_audio_analysis_from_spotify_api(uri_list)
    logging.info(f"Audio analysis thread has finished.")
    print(f"Main    : Audio analysis thread has finished.")


def create_and_start_threads(uris):
    logging.info(f"Main    : before creating threads")
    print(f"Main    : before creating threads")
    features_thread = threading.Thread(
        target=thread_audio_features_from_spotify_api, args=(uris,)
    )
    analysis_thread = threading.Thread(target=thread_audio_analysis, args=(uris,))

    logging.info(f"Main    : before running threads")
    print(f"Main    : before running threads")

    features_thread.start()
    analysis_thread.start()


def setup_logger():
    logging.basicConfig(
        filename="batched_audio_features_log.log",
        encoding="utf-8",
        level=logging.DEBUG,
        format="%(levelname)s:%(message)s - %(asctime)s ",
    )


def run():
    setup_logger()
    logging.info(f"Program has started.")
    print(f"Program has started.")
    uris = prepare_uris_for_dataset()
    create_and_start_threads(uris)
    logging.info(f"Main    : waiting for the threads to finish...")
    print(f"Main    : waiting for the threads to finish...")


# add try exceptions for stack tracing error logging too
# https://realpython.com/python-logging/#:~:text=Handler%20%3A%20Handlers%20send%20the%20LogRecord,stdout%20or%20a%20disk%20file
if __name__ == "__main__":
    sp = initialize_spotipy()
    run()
