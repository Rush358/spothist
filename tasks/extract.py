import os
import json
from datetime import datetime
from pathlib import Path

from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from prefect import task

from settings import client_id, client_secret
from definitions import ROOT_DIR

@task
def create_oauth_manager(scope: str) -> object:
    """
    Takes in user credentials and  scope then creates and returns OAuth flow manager, limited to specified scope.
    """

    redirect_uri = 'http://localhost:8888/'

    oauth_manager = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri,
                                 scope=scope)

    return oauth_manager


@task
def create_client_credentials_manager() -> object:
    """
    Takes in user credentials then creates and returns client credentials flow manager.
    """

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)

    return client_credentials_manager


@task
def create_spotify_client(auth_manager: object) -> Spotify:
    """
    Takes in auth manager and returns Spotify client with appropriate permissions.
    """
    sp = Spotify(auth_manager=auth_manager)

    return sp

@task
def get_listening_history(sp_lh: object, limit: int = 5) -> Spotify:
    """
    Takes in parameter and returns a dictionary with the last specified number of tracks (defaulting to last five).
    Saves a snapshot of the extract to a JSON file.
    """

    results = sp_lh.current_user_recently_played(limit=limit)

    return results


@task
def get_artist(sp_art: object, artists: list) -> dict:
    """
    Takes in parameter and returns a dictionary with artist information.
    Saves a snapshot of the extract to a JSON file.
    """

    artists = list(set(artists))  # Remove duplicate artists
    results = sp_art.artists(artists)

    return results


@task
def write_dict_to_json(dictionary: dict, directory: str, filename: str):
    """
    Takes in (Python) dict and writes it as a JSON file in a specified directory.
    """

    str_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')
    filename_timestamp = f'{filename}_{str_timestamp}.json'

    file_path = ROOT_DIR / directory / filename_timestamp

    with open(file_path, 'w') as json_file:
        json.dump(dictionary, json_file, indent=4)


@task
def build_archive_directory() -> Path:
    str_date = datetime.now().strftime('%Y-%m-%d')
    dir_archive = ROOT_DIR / 'data' / str_date

    if not os.path.exists(dir_archive):
        os.mkdir(dir_archive)

    return dir_archive


if __name__ == '__main__':
    create_oauth_manager.run(scope='user-read-recently-played')
    # directory_archive = build_archive_directory.run()
    # sp_lh = create_oauth_manager.run()
    # dict_lh = get_listening_history.run(sp_lh, limit=1)
    # write_dict_to_json(dict_lh, directory_archive)
