import prefect
from prefect import task
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth, SpotifyClientCredentials

from settings import sp_client_id, sp_client_secret
from utils.logger import logger_handler


@task
def create_oauth_manager(scope: str) -> SpotifyOAuth:
    """
    Takes in user credentials and  scope then creates and returns OAuth flow manager, limited to specified scope.
    """

    # Get Prefect logger and set custom properties
    customer_handler = logger_handler()
    task_logger = prefect.context.get('logger')
    task_logger.addHandler(customer_handler)

    # Start logging
    # task_logger.info('Start logging')

    redirect_uri = 'http://localhost:8888/'

    return SpotifyOAuth(client_id=sp_client_id, client_secret=sp_client_secret, redirect_uri=redirect_uri, scope=scope)


@task
def create_client_credentials_manager() -> SpotifyClientCredentials:
    """
    Takes in user credentials then creates and returns client credentials flow manager.
    """

    return SpotifyClientCredentials(client_id=sp_client_id, client_secret=sp_client_secret)


@task
def create_spotify_client(auth_manager: SpotifyOAuth) -> Spotify:
    """
    Takes in auth manager and returns Spotify client with appropriate permissions.
    """

    return Spotify(auth_manager=auth_manager)


@task
def get_listening_history(sp: Spotify, limit: int, after: float = None) -> dict:
    """
    Takes in parameter and returns a dictionary with the last specified number of tracks (defaulting to last five).
    Saves a snapshot of the extract to a JSON file.
    """

    return sp.current_user_recently_played(limit=limit, after=after)


@task
def get_artist(sp: Spotify, artists: list) -> dict:
    """
    Takes in parameter and returns a dictionary with artist information.
    Saves a snapshot of the extract to a JSON file.
    """

    if artists:
        return sp.artists(set(artists))  # Remove duplicate artists from request
    else:
        return dict()


if __name__ == '__main__':
    pass
    # client_credentials_manager = create_client_credentials_manager.run()
    # sp_art = create_spotify_client.run(auth_manager=client_credentials_manager)
    # dict_art = get_artist.run(sp=sp_art, artists=['6l3HvQ5sa6mXTsMTB19rO5'])
