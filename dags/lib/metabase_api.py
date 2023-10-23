import requests

class MetabaseAPI:
    '''
    Class to interact with Metabase API
    with the help of Metabase API we can create pulse and send report
    '''
    def __init__(self, username, password):
        # Initialize session and headers
        self.session_id = self._authenticate(username, password)
        self.headers = {'X-Metabase-Session': self.session_id}

    def _authenticate(self, username, password):
        # Authenticate with Metabase
        secret = requests.post('http://host.docker.internal:3000/api/session',
                               json={'username': username, 'password': password})
        return secret.json()['id']

    def create_pulse(self, pulse_data):
        # Post pulse data to Metabase
        response = requests.post('http://host.docker.internal:3000/api/pulse/test', headers=self.headers,
                                 json=pulse_data)
        return response.json()

    @staticmethod 
    def send_report():
        # This is pulse data that we want to send to Metabase
        pulse_data = {
        "name": "dibimbing-final-project",
        "cards": [
            {
            "id": 8,
            "collection_id": 3,
            "description": None,
            "display": "bar",
            "name": "How Age is distributed",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 54,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 9,
            "collection_id": 3,
            "description": None,
            "display": "row",
            "name": "Per Gender",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 55,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 7,
            "collection_id": 3,
            "description": None,
            "display": "line",
            "name": "New Dim User per month",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 56,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 6,
            "collection_id": 3,
            "description": None,
            "display": "line",
            "name": "New Dim User per month",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 57,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 12,
            "collection_id": 4,
            "description": None,
            "display": "row",
            "name": "Dim Ads per Ads ID",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 58,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 13,
            "collection_id": 4,
            "description": None,
            "display": "row",
            "name": "Dim Ads per Device Type",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 59,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 37,
            "collection_id": 6,
            "description": None,
            "display": "line",
            "name": "Fact Daily Event Performance by Timestamp",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 60,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 39,
            "collection_id": 6,
            "description": None,
            "display": "bar",
            "name": "Timestamp by day of the week",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 61,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 38,
            "collection_id": 6,
            "description": None,
            "display": "line",
            "name": "Total Purchase Amount over time",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 62,
            "dashboard_id": 5,
            "parameter_mappings": []
            },
            {
            "id": 17,
            "collection_id": 5,
            "description": None,
            "display": "bar",
            "name": "Fact User Performance by Total Purchase Amount",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 63,
            "dashboard_id": 5,
            "parameter_mappings": []
            }
        ],
        "channels": [
            {
            "channel_type": "email",
            "enabled": True,
            "recipients": [
                {
                "email": "muhammadmuhidin222@gmail.com",
                "first_name": "Muhammad",
                "locale": None,
                "last_login": "2023-10-23T11:22:44.551312Z",
                "is_active": True,
                "is_qbnewb": True,
                "updated_at": "2023-10-23T11:24:23.796632",
                "is_superuser": True,
                "login_attributes": None,
                "id": 1,
                "last_name": "Muhidin",
                "date_joined": "2023-10-23T10:44:57.988428Z",
                "sso_source": None,
                "common_name": "Muhammad Muhidin"
                }
            ],
            "details": {},
            "schedule_type": "hourly",
            "schedule_day": "mon",
            "schedule_hour": 8,
            "schedule_frame": "first"
            }
        ],
        "skip_if_empty": True,
        "collection_id": None,
        "parameters": [],
        "dashboard_id": 5
        }
            
        metabase_api = MetabaseAPI('muhammadmuhidin222@gmail.com', 'Metabase94')
        result = metabase_api.create_pulse(pulse_data)
        print(result)

def main():
    # Testing Purpose
    metabase_api = MetabaseAPI('muhammadmuhidin222@gmail.com', 'Metabase94')
    result = metabase_api.create_pulse(pulse_data)
    print(result)
if __name__== "__main__":
    main ()
