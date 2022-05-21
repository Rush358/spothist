from math import floor
from itertools import zip_longest

import pandas as pd
from prefect import task


@task
def flatten_listening_history_dict(dict: dict) -> dict:  #TODO: pd json explode?
    """
    Extract required fields from listening history nested dict and write to flattened dict.
    """

    dict_keys = ['track_id', 'artist_id', 'album_id', 'artist_name', 'album_name', 'track_name', 'track_duration_ms',
                 'release_date', 'played_at', 'track_number']
    flat_dict = {key: [] for key in dict_keys}

    for track in dict['items']:
        track_details = track['track']

        flat_dict['track_id'].append(track_details['id'])
        flat_dict['artist_id'].append(track_details['artists'][0]['id'])  # Takes primary artist id
        flat_dict['album_id'].append(track_details['album']['id'])
        flat_dict['artist_name'].append(track_details['artists'][0]['name'])  # Takes primary artist
        flat_dict['album_name'].append(track_details['album']['name'])
        flat_dict['track_name'].append(track_details['name'])
        flat_dict['track_duration_ms'].append(track_details['duration_ms'])
        flat_dict['release_date'].append(track_details['album']['release_date'])
        flat_dict['played_at'].append(track['played_at'])
        flat_dict['track_number'].append(track_details['track_number'])

    return flat_dict


@task
def flatten_artist_dict(dict: dict) -> dict:
    """
    Extract required fields from artist nested dict and write to flattened dict.
    """

    dict_keys = ['artist_id', 'artist_name', 'genres']
    dict_art = {key: [] for key in dict_keys}

    for artist in dict['artists']:
        dict_art['artist_id'].append(artist['id'])
        dict_art['artist_name'].append(artist['name'])  # Takes primary artist
        dict_art['genres'].append(artist['genres'])

    return dict_art

@task
def expand_genres(dict_art: dict) -> dict:

    # TODO handle 5 genres
    list_genres = dict_art['genres']
    genre_1, genre_2, genre_3, genre_4, genre_5 = (_ for _ in zip_longest(*list_genres, fillvalue=None))

    flat_dict = dict()
    flat_dict['artist_id'] = dict_art['artist_id']
    flat_dict['artist_name'] = dict_art['artist_name']
    flat_dict['genre_1'] = list(genre_1)
    flat_dict['genre_2'] = list(genre_2)
    flat_dict['genre_3'] = list(genre_3)
    flat_dict['genre_4'] = list(genre_4)
    flat_dict['genre_5'] = list(genre_5)

    return flat_dict

@task
def dict_to_df(dict: dict) -> pd.DataFrame:
    """
    Create dataframe from dict.
    """

    df = pd.DataFrame.from_dict(dict, orient='columns', dtype=str)

    return df



