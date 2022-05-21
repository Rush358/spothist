import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine
from prefect import task

from definitions import CONFIG_PATH
from settings import username, password


@task
def get_configs():
    with open(CONFIG_PATH, 'r') as yml_file:
        config = yaml.safe_load(yml_file)

    return config


@task
def create_db_engine(config: dict) -> object:
    """
    Takes in db configs and returns an engine to connect to Postgres db.
    """
    db_config_stg = config['db_staging']

    dialect = db_config_stg['dialect']
    driver = db_config_stg['driver']
    host = db_config_stg['host']
    port = db_config_stg['port']
    database = db_config_stg['database']

    conn_str = f'{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_str)

    return engine


@task
def load_df_to_sql(df: pd.DataFrame, schema: str, table: str, engine: object):
    """
    Takes in dataframe, database connection to write data to specified target table in Postgres db.
    """
    with engine.begin() as connection:
        df.to_sql(table, con=engine, schema=schema, if_exists='replace', index=False, chunksize=1000, method='multi')


def query_db():

    # Create connection
    conn = psycopg2.connect(database='spothist', user='postgres', password='postmanpat', host='127.0.0.1', port='5432')
    cursor = conn.cursor()

    # Query
    cursor.execute('SELECT * FROM staging.track_history')
    data = cursor.fetchone()

    # Closing the connection
    conn.close()

    return data


@task
def write_to_db(config: dict, df: pd.DataFrame, schema: str, table: str):
    """
    Takes in db configs and...
    """
    db_config_stg = config['db_staging']

    dialect = db_config_stg['dialect']
    driver = db_config_stg['driver']
    host = db_config_stg['host']
    port = db_config_stg['port']
    database = db_config_stg['database']

    conn_str = f'{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_str)

    with engine.begin() as connection:
        df.to_sql(table, con=engine, schema=schema, if_exists='replace', index=False, chunksize=1000, method='multi')

if __name__ == '__main__':
    get_configs.run()
