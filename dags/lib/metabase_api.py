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
            "dashboard_card_id": 69,
            "dashboard_id": 6,
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
            "dashboard_card_id": 70,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 10,
            "collection_id": 3,
            "description": None,
            "display": "row",
            "name": "Per Ads Source",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 71,
            "dashboard_id": 6,
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
            "dashboard_card_id": 72,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 15,
            "collection_id": 4,
            "description": None,
            "display": "line",
            "name": "Dim Ads by Timestamp",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 73,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 14,
            "collection_id": 4,
            "description": None,
            "display": "row",
            "name": "Dim Ads per Ads ID",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 74,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 16,
            "collection_id": 4,
            "description": None,
            "display": "bar",
            "name": "Timestamp by day of the week",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 75,
            "dashboard_id": 6,
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
            "dashboard_card_id": 76,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 49,
            "collection_id": 7,
            "description": None,
            "display": "line",
            "name": "Fact Daily Event Performance by Timestamp",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 77,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 51,
            "collection_id": 7,
            "description": None,
            "display": "bar",
            "name": "Timestamp by day of the week",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 78,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 29,
            "collection_id": 6,
            "description": None,
            "display": "bar",
            "name": "Fact User Performance by Total Purchase Amount",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 79,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 34,
            "collection_id": 6,
            "description": None,
            "display": "line",
            "name": "Total Purchase Amount over time",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 80,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 23,
            "collection_id": 5,
            "description": None,
            "display": "bar",
            "name": "How the Transaction Date is distributed",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 81,
            "dashboard_id": 6,
            "parameter_mappings": []
            },
            {
            "id": 24,
            "collection_id": 5,
            "description": None,
            "display": "bar",
            "name": "Amount by Transaction Date",
            "include_csv": False,
            "include_xls": False,
            "dashboard_card_id": 83,
            "dashboard_id": 6,
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
                "last_login": "2023-10-24T15:01:41.025463Z",
                "is_active": True,
                "is_qbnewb": True,
                "updated_at": "2023-10-24T15:06:23.42499",
                "is_superuser": True,
                "login_attributes": None,
                "id": 1,
                "last_name": "Muhidin",
                "date_joined": "2023-10-24T14:57:58.910205Z",
                "sso_source": None,
                "common_name": "Muhammad Muhidin"
                }
            ],
            "details": {},
            "schedule_type": "daily",
            "schedule_day": None,
            "schedule_hour": 8,
            "schedule_frame": None
            }
        ],
        "skip_if_empty": None,
        "collection_id": None,
        "parameters": [],
        "dashboard_id": 6
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
