import os
from dotenv import load_dotenv

load_dotenv()
sp_client_id = os.environ['sp_client_id']
sp_client_secret = os.environ['sp_client_secret']
pgdb_username = os.environ['pgdb_username']
pgdb_password = os.environ['pgdb_password']
