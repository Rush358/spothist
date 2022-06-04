import pandas as pd
import psycopg2
import yaml
from sqlalchemy import create_engine
from prefect import task

from definitions import CONFIG_PATH
from settings import pgdb_username, pgdb_password
from tasks.database import Postgres


@task
def get_configs():
    with open(CONFIG_PATH, 'r') as yml_file:
        configs = yaml.safe_load(yml_file)

    return configs


@task
def write_to_db(config: dict, df: pd.DataFrame, schema: str, table: str):
    """
    Takes in db configs and...
    """
    db_config_stg = config['db_spothist']

    dialect = db_config_stg['dialect']
    driver = db_config_stg['driver']
    host = db_config_stg['host']
    port = db_config_stg['port']
    database = db_config_stg['database']

    conn_str = f'{dialect}+{driver}://{pgdb_username}:{pgdb_password}@{host}:{port}/{database}'
    engine = create_engine(conn_str)

    with engine.begin() as connection:
        df.to_sql(table, con=engine, schema=schema, if_exists='replace', index=False, chunksize=1000, method='multi')


@task
def write_df(df: pd.DataFrame, configs, database: str, schema: str, table: str):

    with Postgres(configs=configs, database=database) as pg:
        pg.write_df(df, schema, table)


if __name__ == '__main__':
    pass
