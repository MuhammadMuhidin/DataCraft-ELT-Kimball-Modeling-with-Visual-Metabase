from faker import Faker
import polars as pl
import json

class FakerGenerators:
    # Initialize number of records to generate
    def __init__(self, num_records=1000):
        self.fake = Faker()
        self.num_records = num_records

    def generate_users(self):
        # Create a dictionary to store the data
        data = {
            'id': [],
            'first_name': [],
            'last_name': [],
            'email': [],
            'dob': [],
            'gender': [],
            'register_date': [],
            'client_id': []
        }

        # Generate the data and append it to the dictionary
        for _ in range(self.num_records):
            data['id'].append(self.fake.unique.pyint())
            data['first_name'].append(self.fake.first_name())
            data['last_name'].append(self.fake.last_name())
            data['email'].append(self.fake.email())
            data['dob'].append(self.fake.date_of_birth())
            data['gender'].append(self.fake.random_element(elements=('Male', 'Female')))
            data['register_date'].append(self.fake.date_this_century(before_today=True))
            data['client_id'].append(self.fake.pyint())

        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_parquet('/data/raw/users.parquet')
    
    def generate_user_events(self):
        data = {
           'id': [],
           'user_id': [],
           'event_type': [],
           'timestamp': [],
           'device_type': [],
           'location': [],
           'event_data': []
           }

        # Generate the data and append it to the dictionary
        for _ in range(self.num_records):
           data['id'].append(self.fake.unique.pyint())
           data['user_id'].append(self.fake.pyint())
           data['event_type'].append(self.fake.word())
           data['timestamp'].append(self.fake.date_time_this_decade().strftime("%Y-%m-%d %H:%M:%S"))
           event_data = {'device_type': self.fake.random_element(elements=('Mobile', 'Desktop', 'Tablet')),'location': self.fake.city()}
           data['device_type'].append(event_data['device_type'])
           data['location'].append(event_data['location'])
           data['event_data'].append(json.dumps(event_data))

        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_parquet('/data/raw/user_events.parquet')

    def generate_user_transactions(self):
        # Create a dictionary to store the data
        data = {
        'id': [],
        'user_id': [],
        'transaction_date': [],
        'amount': [],
        'transaction_type': 'purchase',
        }

        # Generate the data and append it to the dictionary
        for _ in range(self.num_records):
            data['id'].append(self.fake.unique.pyint())
            data['user_id'].append(self.fake.pyint())
            data['transaction_date'].append(self.fake.date())
            data['amount'].append(self.fake.pyint())

        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_parquet('/data/raw/user_transactions.parquet')

    def generate_facebook_ads(self):
        data = {
            'id': [],
            'ads_id': [],
            'device_type': [],
            'device_id': [],
            'timestamp': [],
            }

        # Generate the data and append it to the dictionary
        for _ in range(self.num_records):
            data['id'].append(self.fake.unique.pyint())
            data['ads_id'].append(self.fake.random_element(elements=('lebaran_campaign', 'imlek_campaign', 'natal_campaign')))
            data['device_type'].append(self.fake.random_element(elements=('Desktop', 'Android', 'IOS')))
            data['device_id'].append(self.fake.ipv4())
            data['timestamp'].append(self.fake.date_time())

        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_csv('/data/raw/facebook_ads.csv')

    def generate_instagram_ads(self):
        # Create a dictionary to store the data
        data = {
            'id': [],
            'ads_id': [],
            'device_type': [],
            'device_id': [],
            'timestamp': [],
        }

        # Generate the data and append it to the dictionary
        for _ in range(self.num_records):
            data['id'].append(self.fake.unique.pyint())
            data['ads_id'].append(self.fake.random_element(elements=('lebaran_campaign', 'imlek_campaign', 'natal_campaign')))
            data['device_type'].append(self.fake.random_element(elements=('Desktop', 'Android', 'IOS')))
            data['device_id'].append(self.fake.ipv4())
            data['timestamp'].append(self.fake.date_time())

        # Write the data to a parquet file
        df = pl.DataFrame(data)
        df.write_csv('/data/raw/instagram_ads.csv')

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
