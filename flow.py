from datetime import datetime, timedelta

from prefect import Flow
from prefect.tasks.dbt import DbtShellTask
from prefect.schedules import IntervalSchedule

from tasks.extract import auth_and_connect_listening_history, auth_and_connect_artist, get_listening_history,\
    get_artist, build_archive_directory, write_dict_to_json
from tasks.transform import flatten_listening_history_dict, flatten_artist_dict, dict_to_df
from tasks.load import get_configs, create_db_engine, load_df_to_sql, write_to_db

# schedule = IntervalSchedule(start_date=datetime.now() + timedelta(seconds=10),
#                             interval=timedelta(minutes=10))

with Flow('spothist-etl') as flow_etl:

    # Extract / transform
    sp_lh = auth_and_connect_listening_history()  # Authenticate and access
    dict_lh = get_listening_history(sp_lh, limit=5)  # Extract
    dict_lh_flat = flatten_listening_history_dict(dict_lh)  # Transform

    sp_art = auth_and_connect_artist()  # Authenticate and access
    dict_art = get_artist(sp_art, artists=dict_lh_flat['artist_id'])  # Extract artists in history
    dict_art_flat = flatten_artist_dict(dict_art)  # Transform

    dir_archive = build_archive_directory()  # Create archive directory
    write_dict_to_json(dictionary=dict_art, directory=dir_archive)  # Write raw responses to directory
    write_dict_to_json(dictionary=dict_lh, directory=dir_archive)

    # Transform
    df_lh = dict_to_df(dict_lh_flat)
    df_art = dict_to_df(dict_art_flat)

    #  Load
    db_configs = get_configs()
    # load_df_to_sql(df_lh, schema='staging', table='listening_history', engine=engine)
    # load_df_to_sql(df_art, schema='staging', table='artist', engine=engine)
    write_to_db(db_configs, df_lh, schema='staging', table='listening_history')
    write_to_db(db_configs, df_art, schema='staging', table='artist')

if __name__ == '__main__':
    flow_etl.run()
    # flow_etl.register(project_name='spothist-etl')

# Project
# TODO: Implement data validation: what happens when API result is blank etc.
# TODO: Setup config file for tables as operations are the same
# TODO: Add log file to track last run and timestamp - extract after that timestamp

# Prefect
# TODO: Set up a log file which tracks last run Prefect?
# TODO: Get Prefect dbt task

# Structure/packaging/cloud
# TODO: Set up GitHub branches etc.
# TODO: Create Docker image of project
