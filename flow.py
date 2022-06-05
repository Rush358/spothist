from prefect import Flow
from prefect.tasks.dbt import DbtShellTask

from utils.logger import get_last_run
from utils.config import get_configs

from tasks.extract import create_oauth_manager, create_client_credentials_manager, create_spotify_client,\
    get_listening_history, get_artist
from tasks.archive import build_archive_directory, write_dict_to_json
from tasks.transform import flatten_listening_history_dict, flatten_artist_dict, dict_to_df
# from tasks.load import write_to_db
from tasks.load import write_df


with Flow('spothist-etl') as flow_etl:

    # Authenticate and set up Spotify clients
    oauth_manager = create_oauth_manager(scope='user-read-recently-played')
    sp_lh = create_spotify_client(auth_manager=oauth_manager, upstream_tasks=[oauth_manager])

    client_credentials_manager = create_client_credentials_manager()
    sp_art = create_spotify_client(auth_manager=client_credentials_manager, upstream_tasks=[client_credentials_manager])

    # Extract / transform
    timestamp_last_run = get_last_run(upstream_tasks=[sp_lh, sp_art])

    dict_lh = get_listening_history(sp=sp_lh, limit=1, after=timestamp_last_run,
                                    upstream_tasks=[sp_lh, timestamp_last_run])
    dict_lh_flat = flatten_listening_history_dict(dict_lh=dict_lh, upstream_tasks=[dict_lh])

    dict_art = get_artist(sp=sp_art, artists=dict_lh_flat['artist_id'], upstream_tasks=[dict_lh_flat])
    dict_art_flat = flatten_artist_dict(dict_art=dict_art, upstream_tasks=[dict_art])

    # dict_lh_flat = flatten_listening_history_dict(dict_lh=dict())
    # dict_art_flat = flatten_artist_dict(dict_art=dict())

    # Archive responses
    dir_archive = build_archive_directory(upstream_tasks=[dict_lh_flat, dict_art_flat])
    write_dict_to_json(dictionary=dict_lh, directory=dir_archive, filename='listening_history',
                       upstream_tasks=[dict_lh, dir_archive])
    write_dict_to_json(dictionary=dict_art, directory=dir_archive, filename='artist',
                       upstream_tasks=[dict_art, dir_archive])

    # Transform
    df_lh = dict_to_df(dictionary=dict_lh_flat, upstream_tasks=[dict_lh_flat])
    df_art = dict_to_df(dictionary=dict_art_flat, upstream_tasks=[dict_art_flat])

    # Load
    configs = get_configs(upstream_tasks=[df_lh, df_art])
    write_df(df=df_lh, configs=configs, database='db_spothist', schema='staging', table='listening_history',
             upstream_tasks=[df_lh, configs])
    write_df(df=df_art, configs=configs, database='db_spothist', schema='staging', table='artist',
             upstream_tasks=[df_art, configs])
    # write_to_db(configs, df_lh, schema='staging', table='listening_history')

    # TODO: Configure logger time to use UTC
    # TODO: Add Prefect dbt tasks once shell can be used
    # TODO: Setup up param flow mapping for archive steps onwards?
    # TODO: Create Docker image of project

if __name__ == '__main__':
    flow_etl.run()
    # flow_etl.visualize()
    # flow_etl.register(project_name='spothist-etl')
