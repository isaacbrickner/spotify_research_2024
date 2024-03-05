import json
from mimetypes import init
import time
import os
import pandas as pd
import time
import logging
import threading
import re
from globals import initialize_spotipy
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from requests.exceptions import ReadTimeout


class InitializeSpotifyCredentials:

    def initialize_spotipy(self):
        load_dotenv()
        sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=os.getenv("CLIENT_ID"),
                client_secret=os.getenv("CLIENT_SECRET"),
                redirect_uri=os.getenv("REDIRECT_URI"),
                scope=os.getenv("SCOPE"),
            ), requests_timeout=30, retries=30
        )
        return sp


class FeatureRetriever:
    def __init__(self, sp):
        self.sp = sp

    def print_playlists(path):
        filenames = os.listdir(path)
        for filename in sorted(filenames):
            if filename.startswith("mpd.slice.") and filename.endswith(".json"):
                fullpath = os.sep.join((path, filename))
                f = open(fullpath)
                js = f.read()
                f.close()
                mpd_slice = json.loads(js)
                for playlist in mpd_slice["playlists"]:
                    _ = json.dumps(playlist)

    # TODO: Log processing of track aggregation - https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters
    def aggregate_tracks_from_playlists(self, path):
        """ This function aggreagates all of the tracks from the 1 million playlists.

        Args:
            path (string): this is the path to the data directory provided by the Spotify Million Playlist.

        Returns:
            List: A list of track dictionaries from the playlist dataset.
        """
        filenames = os.listdir(path)
        aggregated_tracks = []
        for filename in sorted(filenames):
            if filename.startswith("mpd.slice.") and filename.endswith(".json"):
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

    def find_and_remove_duplicate_tracks(self, raw_tracks):
        """ This takes the 66 million tracks from the playlists and removes and duplicates and logs some information.

        Args:
            raw_tracks List: A list of tracks dictionaries containing a name, artist name, and uri.

        Returns:
            List: A list of track dictionaries that are unique. n =~ 2.2m
        """
        logging.info(f"Finding and removing duplicate tracks.") # abstract logging to external functions
        print(f"Finding and removing duplicate tracks.")
        logging.info(f"Raw tracks before filtering: {len(raw_tracks)}")
        print(f"Raw tracks before filtering: {len(raw_tracks)}")
        
        df = pd.DataFrame(raw_tracks)
        df.drop_duplicates(subset=["track_uri"], keep="last", inplace=True)
        filtered_tracks = df.to_dict("records")
        logging.info(f"Unique tracks after removing duplicates: {len(filtered_tracks)}")
        print(f"Unique tracks after removing duplicates: {len(filtered_tracks)}")
        return filtered_tracks

    def create_uri_list(self, tracks):
        """ This creates a List of the URIs from each track.

        Args:
            tracks List: A List of unique tracks.

        Returns:
            List: A list of URIs.
        """
        track_uris = []
        for track in tracks:
            uri = track["track_uri"]
            track_uris.append(uri)
            # write uri list to file
        return track_uris

    def create_uri_file(self, uris):
        """ Creates a text file of the unique URIs.
        
        Args:
            uris List: A List of unique URIs.
        
        
        Return: None
        """
        
        for uri in uris:
            with open("uri_list.txt", "a") as txt_file:
                txt_file.write(f"{uri}\n")

    """ The function below is intended to replace this list comprehension: 
    raw_tracks = [track_uris[i : i + 100] for i in range(0, len(track_uris), 100)] 
    """

    def process_raw_tracks(self, track_uris):
        logging.info(f"Beginning processing batches of raw tracks.")
        batch_size = 100
        raw_tracks = []
        for i in range(0, len(track_uris), batch_size):
            logging.info(f"Processing batch from index {i} to {i + batch_size}")
            batch = track_uris[i : i + batch_size]
            raw_tracks.append(batch)
        return raw_tracks

    def prepare_uris_for_dataset(self):
        raw_tracks = self.aggregate_tracks_from_playlists(
            "./spotify_million_playlist_dataset/data"
        )

        unique_tracks = self.find_and_remove_duplicate_tracks(raw_tracks)
        print(f"Duplicates removed.")

        uri_list = self.create_uri_list(unique_tracks)
        self.create_uri_file(uri_list)
        return uri_list

    # TODO: need to take out each uri for a processed track in a batch
    # and save it to another called processed. only store it if the entire features data was
    # actually stored. otherwise do it again. then start from there.

    def get_batched_audio_features_from_spotify_api(self, track_uris):
        sp = self.sp
        audio_features = []
        raw_tracks = self.process_raw_tracks(track_uris)
        outfile = open("audio_features.json", "w")
        processed_output_file = "processed_uri.txt"
        pattern = r"spotify:track:[a-zA-Z0-9]{22}"
        start_time = time.time()
        for index, batch in enumerate(raw_tracks):
            self.print_elapsed_time(start_time)
            logging.info(f"Getting features for batch: {index}")
            try:
                features = sp.audio_features(batch)
            except ReadTimeout:
                print('Spotify timed out... trying again...')
                features = sp.audio_features(batch)
            audio_features.append(features)
            time.sleep(5.0)

            for feature in features:
                json.dump(feature, outfile)
                outfile.write(",\n")
            for uri in features:
                matches = re.findall(pattern, str(uri))
                with open(processed_output_file, "a") as txt_file:
                    txt_file.write(f"{str(matches)}\n")
        outfile.close()

    # TODO: Get analysis later. not needed yet.

    # def get_audio_analysis_from_spotify_api(uri_list):
    #     audio_analysis_list = []
    #     outfile = open("audio_analysis_dataset.json", "w")
    #     for uri in uri_list:
    #         song_analysis = sp.audio_analysis(uri)
    #         time.sleep(4.5)
    #         # audio_analysis_list.append(
    #         #     song_analysis
    #         # )  # do i need the data structure? or can i just write to the file?
    #         json.dump(song_analysis, outfile)
    #     outfile.close()

    def thread_audio_features_from_spotify_api(self, uri_list):

        logging.info(f"Audio features thread starting...")
        self.get_batched_audio_features_from_spotify_api(uri_list)
        logging.info(f"Audio features thread has finished.")
        print(f"Main    : Audio features thread has finished.")

    # TODO: uncomment when doing audio analysis features

    # def thread_audio_analysis(uri_list):
    #     logging.info(f"Audio analysis thread starting...")
    #     get_audio_analysis_from_spotify_api(uri_list)
    #     logging.info(f"Audio analysis thread has finished.")
    #     print(f"Main    : Audio analysis thread has finished.")

    # TODO: uncomment when needing to multithread

    def create_and_start_threads(self, uris):
        logging.info(f"Main    : before creating threads")
        print(f"Main    : before creating threads")
        features_thread = threading.Thread(
            target=self.thread_audio_features_from_spotify_api, args=(uris,)
        )
        # analysis_thread = threading.Thread(target=thread_audio_analysis, args=(uris,))
        # start_time = time.time()
        logging.info(f"Main    : before running threads")
        print(f"Main    : before running threads")
        features_thread.start()
        # analysis_thread.start()
        # while analysis_thread.is_alive() and features_thread.is_alive():
        #     print_elapsed_time(start_time)

    def setup_logger(self):
        logging.basicConfig(
            filename="batched_audio_features_log.log",
            encoding="utf-8",
            level=logging.DEBUG,
            format="%(levelname)s:%(message)s - %(asctime)s ",
        )

    def print_elapsed_time(self, start_time, interval=2):
        current_time = time.time()
        elapsed_time_seconds = current_time - start_time
        elapsed_hours, remaining_seconds = divmod(elapsed_time_seconds, 3600)
        elapsed_minutes, remaining_seconds = divmod(remaining_seconds, 60)
        elapsed_seconds, elapsed_milliseconds = divmod(remaining_seconds, 1)
        print(
            f"Elapsed Time since threads started: {int(elapsed_hours):02}:{int(elapsed_minutes):02}:{int(elapsed_seconds):02}.{int(elapsed_milliseconds * 1000):03}"
        )
        time.sleep(interval)

    def run(self):
        # run program, start by trying to see if the uri list file is empty or not. 

        try:
            with open("uri_list.txt", "r+") as f:
                f.seek(0)
                if not f.read():
                    print("No uri's have been logged")
                    self.setup_logger()
                    logging.info(f"Program has started.")
                    print(f"Program has started.")
                    uris = self.prepare_uris_for_dataset()
                    self.thread_audio_features_from_spotify_api(uris)
                    logging.info(f"Main    : waiting for the threads to finish...")
                    print(f"Main    : waiting for the threads to finish...")
        except: # file is not empty -> find matching uri and start procesing from that point
            with open("uri_list.txt", "r+") as f:
                uri_parallel_list = []
                f.seek(0) 
                if f.read(): # check if file has lines written to it. 
                    # get last line uri from the text file
                    last_line = f.readlines()[-1]
                    
                for i, line in f: # perhaps store into a data structure to then process from that point forward?
                    if last_line == line:  # find it in the uri_list
                        starting_uri = line[i + 1]  # go up by one line and start processing from `n + 1`` since `n` has already been processed.
                        # create new list from that starting point, then pass it in
                        
        # TODO: create dictionary of uris to check processing. key: spotify:uri:xxxxxxxxx, value: 1 or 0 denoting if it has been processed or not.


# TODO: add try exceptions for stack tracing error logging too
# TODO: https://realpython.com/python-logging/#:~:text=Handler%20%3A%20Handlers%20send%20the%20LogRecord,stdout%20or%20a%20disk%20file
if __name__ == "__main__":
    init_credentials = InitializeSpotifyCredentials()
    spotify_features = FeatureRetriever(init_credentials.initialize_spotipy())
    spotify_features.run()
