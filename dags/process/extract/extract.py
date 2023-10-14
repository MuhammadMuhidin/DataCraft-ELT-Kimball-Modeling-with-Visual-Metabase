from google.cloud import storage
from io import BytesIO
import polars as pl
import os

class Extract:
    def __init__(self, BUCKET_NAME, CRED_PATH, DATA_PATH):
        self.bucket_name = BUCKET_NAME
        self.credentials_path = CRED_PATH
        self.data_path = DATA_PATH

    def extract_processing(self):
        # Initialize client and bucket for google cloud storage
        client = storage.Client.from_service_account_json(self.credentials_path)
        bucket = client.get_bucket(self.bucket_name)
        blobs = bucket.list_blobs()

        for blob in blobs:
            # Search parquet and csv file
            # if found then download_as_bytes and write to json
            if blob.name.endswith('.parquet'):
                parquet_byte = blob.download_as_bytes()
                df = pl.read_parquet(BytesIO(parquet_byte))
                extract_to = os.path.join(self.data_path, blob.name[:-8]+'.json')
                df.write_json(file=extract_to, row_oriented=True)
                print(f"Successfully extract parquet to json and saved in {extract_to}")
            elif blob.name.endswith('.csv'):
                csv_byte = blob.download_as_bytes()
                df = pl.read_csv(BytesIO(csv_byte))
                extract_to = os.path.join(self.data_path, blob.name[:-4]+'.json')
                df.write_json(file=extract_to, row_oriented=True)
                print(f"Successfully extract csv to json and saved in {extract_to}")
