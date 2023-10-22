from faker import Faker
import polars as pl
import json

class FakerGenerators:
    '''
    Class to generate fake data, such as users, user_events, user_transactions, facebook_ads, instagram_ads.
    with the help of Faker generate 1000 rows of data.
    output : csv, parquet
    '''
    def __init__(self, num_records=1000):
        self.fake = Faker()
        self.num_records = num_records

    def _generate_data(self, fields, data_function):
        # Create an empty dictionary
        data = {field: [] for field in fields}

        # Generate data for each field
        for _ in range(self.num_records):
            record_data = data_function()
            for field, value in record_data.items():
                data[field].append(value)

        # Return the generated data        
        return data

    def _write_csv(self, data, filename):
        # Write the data to a CSV file
        df = pl.DataFrame(data)
        df.write_csv(filename)

    def _write_parquet(self, data, filename):
        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_parquet(filename)

    def generate_users(self):
        # Define the fields to generate
        fields = ['id', 'first_name', 'last_name', 'email', 'dob', 'gender', 'register_date', 'client_id']

        # Define a function to generate users data
        def data_function():
            return {
                'id': self.fake.unique.pyint(),
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'dob': self.fake.date_of_birth(tzinfo=None, minimum_age=18, maximum_age=50),
                'gender': self.fake.random_element(elements=('Male', 'Female')),
                'register_date': self.fake.date_this_century(before_today=True),
                'client_id': self.fake.pyint()
            }

        # Generate user data
        data = self._generate_data(fields, data_function)
        self._write_parquet(data, '/data/raw/users.parquet')

    def generate_user_events(self):
        # Define the fields to generate
        fields = ['id', 'user_id', 'event_type', 'timestamp', 'device_type', 'location', 'event_data']

        # Define a function to generate user_events data
        def data_function():
            # Define the event data
            event_data = {
                'device_type': self.fake.random_element(elements=('Mobile', 'Desktop', 'Tablet')),
                'location': self.fake.city(),
                'event_type': self.fake.random_element(elements=('login', 'search', 'purchase'))
            }

            # Return the event data
            return {
                'id': self.fake.unique.pyint(),
                'user_id': self.fake.pyint(),
                'event_type': self.fake.word(),
                'timestamp': self.fake.date_time_this_decade(before_now=True, after_now=False).strftime("%Y-%m-%d %H:%M:%S"),
                'device_type': event_data['device_type'],
                'location': event_data['location'],
                'event_data': json.dumps(event_data)
            }

        # Generate user_events data
        data = self._generate_data(fields, data_function)
        self._write_parquet(data, '/data/raw/user_events.parquet')

    def generate_user_transactions(self):
        # Define the fields to generate
        fields = ['id', 'user_id', 'transaction_date', 'amount', 'transaction_type']

        # Define a function to generate user_transactions data
        def data_function():
            return {
                'id': self.fake.unique.pyint(),
                'user_id': self.fake.pyint(),
                'transaction_date': self.fake.date_between(start_date='-1y', end_date='today'),
                'amount': self.fake.pyint(),
                'transaction_type': 'purchase'
            }

        # Generate user_transactions data
        data = self._generate_data(fields, data_function)
        self._write_parquet(data, '/data/raw/user_transactions.parquet')

    def generate_facebook_ads(self):
        # Define the fields to generate
        fields = ['id', 'ads_id', 'device_type', 'device_id', 'timestamp']

        # Define a function to generate facebook_ads data
        def data_function():
            return {
                'id': self.fake.unique.pyint(),
                'ads_id': self.fake.random_element(elements=('lebaran_campaign', 'imlek_campaign', 'natal_campaign')),
                'device_type': self.fake.random_element(elements=('Desktop', 'Android', 'IOS')),
                'device_id': self.fake.ipv4(),
                'timestamp': self.fake.date_time()
            }

        # Generate facebook_ads data
        data = self._generate_data(fields, data_function)
        self._write_csv(data, '/data/raw/facebook_ads.csv')

    def generate_instagram_ads(self):
        # Define the fields to generate
        fields = ['id', 'ads_id', 'device_type', 'device_id', 'timestamp']

        # Define a function to generate instagram_ads data
        def data_function():
            return {
                'id': self.fake.unique.pyint(),
                'ads_id': self.fake.random_element(elements=('lebaran_campaign', 'imlek_campaign', 'natal_campaign')),
                'device_type': self.fake.random_element(elements=('Desktop', 'Android', 'IOS')),
                'device_id': self.fake.ipv4(),
                'timestamp': self.fake.date_time()
            }

        # Generate instagram_ads data
        data = self._generate_data(fields, data_function)
        self._write_csv(data, '/data/raw/instagram_ads.csv')

    @staticmethod
    def create():
      # Create a Faker instance
      # call all the functions
      fake = FakerGenerators()
      fake.generate_users()
      fake.generate_user_events()
      fake.generate_user_transactions()
      fake.generate_facebook_ads()
      fake.generate_instagram_ads()

def main():
    # Unit Testing Purpose
    fake = FakerGenerators()
    fake.create()
if __name__== "__main__":
    main ()
