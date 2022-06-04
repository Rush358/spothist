import pandas as pd
import psycopg2

from sqlalchemy import create_engine

from settings import pgdb_username, pgdb_password
from utils.config import get_configs


class Postgres:
    def __init__(self, configs: dict, database: str):
        self.config = configs[database]
        self.dialect = self.config['dialect']
        self.driver = self.config['driver']
        self.host = self.config['host']
        self.port = self.config['port']
        self.database = self.config['database']
        # self.user = 'postgres' # pgdb_username
        # self.password = 'postmanpat' # pgdb_password

    # def connect(self):
    #     conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host,
    #                             port=self.port)
    #     return conn
    #
    # def __enter__(self):
    #     self.conn = self.connect()
    #     return self
    #
    # def __exit__(self, exc_type, exc_value, traceback):
    #     self.conn.close()
    #
    # def query_db(self, query: str):
    #     conn = self.connect()
    #     cursor = conn.cursor()
    #
    #     cursor.execute(query)
    #     result = cursor.fetchone()
    #
    #     cursor.close()
    #     conn.close()
    #
    #     return result

    def create_engine(self):
        conn_str = f'{self.dialect}+{self.driver}://{pgdb_username}:{pgdb_password}@{self.host}:{self.port}/' \
                   f'{self.database}'
        engine = create_engine(conn_str)

        return engine

    def write_df(self, df: pd.DataFrame, schema: str, table: str):
        engine = self.create_engine()

        with engine.begin() as connection:
            df.to_sql(table, con=engine, schema=schema, if_exists='replace', index=False, chunksize=1000,
                      method='multi')


if __name__ == '__main__':
    configs = get_configs()

    with Postgres(configs=configs, database='db_spothist') as p1:
        data = p1.query_db('SELECT * FROM staging.listening_history ORDER BY played_at DESC')
        print(data)
