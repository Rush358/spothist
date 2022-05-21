import os
import json
from datetime import datetime

import spotipy
from spotipy import Spotify, SpotifyException
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials
from prefect import task

from settings import client_id, client_secret


@task
def build_archive_directory() -> str:
    str_date = datetime.now().strftime('%Y-%m-%d')
    directory_snapshot = f'E:\\Users\\Rushil\\Documents\\Python\\spothist\\data\\{str_date}'

    if not os.path.exists(directory_snapshot):
        os.mkdir(directory_snapshot)

    return directory_snapshot


@task
def auth_and_connect_listening_history() -> object:
    """
    Takes user credentials, authenticates user and returns Spotify API client
    :return: Spotify API client
    """

    redirect_uri = 'http://localhost:8080/'  # TODO Do something with this?
    scope = 'user-read-recently-played'

    auth_manager = SpotifyOAuth(client_id=client_id, client_secret=client_secret, redirect_uri=redirect_uri,
                                scope=scope)
    sp_lh = Spotify(auth_manager=auth_manager)

    return sp_lh


@task
def auth_and_connect_artist() -> object:
    """
    Takes user credentials, authenticates user and returns Spotify API client
    :return: Spotify API client
    """

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp_art = Spotify(client_credentials_manager=client_credentials_manager)

    return sp_art


@task
def get_listening_history(sp_lh: object, limit: int = 5) -> dict:
    """
    Takes in parameter and returns a dictionary with the last specified number of tracks (defaulting to last five).
    Saves a snapshot of the extract to a JSON file.
    """

    results = sp_lh.current_user_recently_played(limit=limit)

    return results


@task
def get_artist(sp_art: object, artists: list) -> dict:  # TODO: Check if there's a limit
    """
    Takes in parameter and returns a dictionary with artist information.
    Saves a snapshot of the extract to a JSON file.
    """

    artists = list(set(artists))  # Remove duplicate artists
    results = sp_art.artists(artists)

    return results


@task
def write_dict_to_json(dictionary: dict, directory: str):
    """
    Takes in (Python) dict and writes it as a JSON file in a specified directory.
    """

    str_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H%M%S')
    file_path = directory + f'\\artists_{str_timestamp}.json'

    with open(file_path, 'w') as json_file:  # TODO change to pathlib
        json.dump(dictionary, json_file, indent=4)


if __name__ == '__main__':
    directory_archive = build_archive_directory.run()
    sp_lh = auth_and_connect_listening_history.run()
    dict_lh = get_listening_history.run(sp_lh, limit=1)
    write_dict_to_json(dict_lh, directory_archive)
