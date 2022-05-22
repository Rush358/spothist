import pandas as pd
from prefect import task


@task
def flatten_listening_history_dict(dictionary: dict) -> dict:
    """
    Extract required fields from listening history nested dict and write to flattened dict.
    """

    dict_keys = ['track_id', 'artist_id', 'album_id', 'artist_name', 'album_name', 'track_name', 'track_duration_ms',
                 'release_date', 'played_at', 'track_number']
    flat_dict = {key: [] for key in dict_keys}

    # Each track's details are added to list which is value in key-value pair
    for track in dictionary['items']:
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
def flatten_artist_dict(dictionary: dict) -> dict:
    """
    Extract required fields from artist nested dict and write to flattened dict.
    """

    dict_keys = ['artist_id', 'artist_name', 'genres']
    dict_art = {key: [] for key in dict_keys}

    # Each artist's details are added to list which is value in key-value pair
    for artist in dictionary['artists']:
        dict_art['artist_id'].append(artist['id'])
        dict_art['artist_name'].append(artist['name'])  # Takes primary artist
        dict_art['genres'].append(artist['genres'])

    return dict_art


@task
def dict_to_df(dictionary: dict) -> pd.DataFrame:
    """
    Create dataframe from dict.
    """

    df = pd.DataFrame.from_dict(dictionary, orient='columns', dtype=str)

    return df


