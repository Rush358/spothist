import pandas as pd
from prefect import task


@task
def flatten_listening_history_dict(dict_lh: dict) -> dict:
    """
    Extract required fields from listening history nested dict and write to flattened dict.
    """

    dict_keys = ['track_id', 'artist_id', 'album_id', 'artist_name', 'album_name', 'track_name', 'track_duration_ms',
                 'release_date', 'played_at', 'track_number']
    dict_lh_flat = {key: [] for key in dict_keys}

    try:
        tracks = dict_lh['items']
    except KeyError:
        print('Missing expected fields in listening history response.')
    else:
        if not len(tracks):
            print('No new listening history since last successful extract - no new data extracted.')
            return dict_lh_flat

    try:
        # Each track's details are added to list which is value in key-value pair
        for track in tracks:
            track_details = track['track']
            dict_lh_flat['track_id'].append(track_details['id'])
            dict_lh_flat['artist_id'].append(track_details['artists'][0]['id'])  # Takes primary artist id
            dict_lh_flat['album_id'].append(track_details['album']['id'])
            dict_lh_flat['artist_name'].append(track_details['artists'][0]['name'])  # Takes primary artist
            dict_lh_flat['album_name'].append(track_details['album']['name'])
            dict_lh_flat['track_name'].append(track_details['name'])
            dict_lh_flat['track_duration_ms'].append(track_details['duration_ms'])
            dict_lh_flat['release_date'].append(track_details['album']['release_date'])
            dict_lh_flat['played_at'].append(track['played_at'])
            dict_lh_flat['track_number'].append(track_details['track_number'])
    except KeyError:
        print('Missing specific track field(s) in listening history response.')
    finally:
        return dict_lh_flat


@task
def flatten_artist_dict(dict_art: dict) -> dict:
    """
    Extract required fields from artist nested dict and write to flattened dict.
    """

    dict_keys = ['artist_id', 'artist_name', 'genres']
    dict_art_flat = {key: [] for key in dict_keys}

    # If there's no listening history then argument is expected to be a blank dict
    if not dict_art:
        print('No new artist information extracted in this run.')
        return dict_art_flat

    try:
        artists = dict_art['artists']
    except KeyError:
        print('Missing expected fields in artist response.')

    try:
        # Each artist's details are added to list which is value in key-value pair
        for artist in artists:
            dict_art_flat['artist_id'].append(artist['id'])
            dict_art_flat['artist_name'].append(artist['name'])  # Takes primary artist
            dict_art_flat['genres'].append(artist['genres'])
    except KeyError:
        print('Missing specific artist field(s) in artist response.')
    finally:
        return dict_art_flat


@task
def dict_to_df(dictionary: dict) -> pd.DataFrame:
    """
    Create dataframe from dict.
    """

    df = pd.DataFrame.from_dict(dictionary, orient='columns', dtype=str)

    return df


if __name__ == '__main__':
    dict_lh_test = {
        "items": [],
        "next": None,
        "cursors": None,
        "limit": 1,
        "href": "https://api.spotify.com/v1/me/player/recently-played?after=1654548840000&limit=1"
    }
    print(flatten_listening_history_dict.run(dict_lh=dict_lh_test))

    dict_art_test = dict()
    print(flatten_artist_dict.run(dict_art=dict_art_test))
