import polars as pl
import os

class Extract:
    '''
    This class is used to extract data from raw folder to extracted folder,
    then export to csv
    '''
    def __init__(self, DATA_PATH):
        # initialize path
        self.path = DATA_PATH

    def extract_processing(self):
        # seacrh file in raw folder then extract to extracted with csv format
        for filename in os.listdir(self.path+'/raw'):
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(self.path+'/raw', filename)
                df = pl.read_csv(csv_file_path)
                extract_to = os.path.join(self.path+'/extracted', filename[:-4]+'.csv')
                df.write_csv(extract_to)
            elif filename.endswith('.parquet'):
                parquet_file_path = os.path.join(self.path+'/raw', filename)
                df = pl.read_parquet(parquet_file_path)
                extract_to = os.path.join(self.path+'/extracted', filename[:-8]+'.csv')
                df.write_csv(extract_to)
