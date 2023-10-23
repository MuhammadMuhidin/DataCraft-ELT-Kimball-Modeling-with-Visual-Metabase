from dotenv import load_dotenv
from pathlib import Path
import polars as pl
import os

class Load:
    def __init__(self, DATA_PATH):
        # Load .env
        dotenv_path = Path('/opt/app/.env')
        load_dotenv(dotenv_path=dotenv_path)

        # Set a variables
        self.data_path = DATA_PATH
        self.postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
        self.postgres_user = os.getenv('POSTGRES_USER')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD')

    def load_procesing(self):
        for filename in os.listdir(self.data_path+'/extracted'):
            # Search json file
            # if found then write to database postgresql
            if filename.endswith('.json'):
                json_file_path = os.path.join(self.data_path+'/extracted', filename)
                df = pl.read_json(json_file_path)
                df.write_database(
                    table_name=filename[:-5],
                    connection='postgresql://'+self.postgres_user+':'+self.postgres_password+'@'+self.postgres_host+'/postgres',
                    if_exists='replace',
                    engine='sqlalchemy'
                )
                print(f"Successfully load data to database.")