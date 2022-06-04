from datetime import datetime, timedelta

from prefect import Flow
from prefect.tasks.dbt import DbtShellTask
from prefect.schedules import IntervalSchedule

from tasks.extract import create_oauth_manager, create_client_credentials_manager, create_spotify_client, get_listening_history, get_artist
from tasks.archive import build_archive_directory, write_dict_to_json
from tasks.transform import flatten_listening_history_dict, flatten_artist_dict, dict_to_df
from tasks.load import write_to_db
from tasks.load import write_df, get_configs

from utils.logger import get_last_run

# schedule = IntervalSchedule(start_date=datetime.now() + timedelta(seconds=10),
#                             interval=timedelta(minutes=10))


with Flow('spothist-etl') as flow_etl:

    # Authenticate and set up Spotify clients
    oauth_manager = create_oauth_manager(scope='user-read-recently-played')
    sp_lh = create_spotify_client(auth_manager=oauth_manager)

    client_credentials_manager = create_client_credentials_manager()
    sp_art = create_spotify_client(auth_manager=client_credentials_manager)

    # Extract / transform
    timestamp_last_run = get_last_run()
    dict_lh = get_listening_history(sp=sp_lh, limit=1, after=timestamp_last_run)
    dict_lh_flat = flatten_listening_history_dict(dictionary=dict_lh)

    dict_art = get_artist(sp=sp_art, artists=dict_lh_flat['artist_id'])
    dict_art_flat = flatten_artist_dict(dictionary=dict_art)

    # Archive responses
    dir_archive = build_archive_directory()  # Create archive directory
    write_dict_to_json(dictionary=dict_art, directory=dir_archive, filename='artist')  # Archive raw files
    write_dict_to_json(dictionary=dict_lh, directory=dir_archive, filename='listening_history')

    # Transform
    df_lh = dict_to_df(dictionary=dict_lh_flat)
    df_art = dict_to_df(dictionary=dict_art_flat)

    # # Load
    configs = get_configs()
    # write_df(df=df_lh, configs=configs, database='db_spothist', schema='staging', table='listening_history')
    # write_df(df=df_art, configs=configs, database='db_spothist', schema='staging', table='artist')
    write_to_db(configs, df_lh, schema='staging', table='listening_history')

if __name__ == '__main__':
    # flow_etl.run()
    flow_etl.register(project_name='spothist-etl')

# Project
# TODO: Implement data validation: what happens when API result is blank etc.
# TODO: Setup config file for tables as operations are the same

# Prefect
# TODO: Change logger time to UTC
# TODO: Get Prefect dbt task

# Structure/packaging/cloud
# TODO: Set up GitHub branches etc.
# TODO: Create Docker image of project
