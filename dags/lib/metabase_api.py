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
                "id": 132,
                "collection_id": 34,
                "description": None,
                "display": "bar",
                "name": "How Age is distributed",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 195,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 87,
                "collection_id": 25,
                "description": None,
                "display": "bar",
                "name": "How the Gender is distributed",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 139,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 89,
                "collection_id": 27,
                "description": None,
                "display": "row",
                "name": "Per Device Type",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 138,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 130,
                "collection_id": 34,
                "description": None,
                "display": "line",
                "name": "New Dim User per month",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 196,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 95,
                "collection_id": 28,
                "description": None,
                "display": "table",
                "name": "Ads ID by Device Type",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 141,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 94,
                "collection_id": 28,
                "description": None,
                "display": "bar",
                "name": "How the Ads ID is distributed",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 140,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 82,
                "collection_id": 20,
                "description": None,
                "display": "row",
                "name": "Facebook Ads per Device Type",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 142,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 83,
                "collection_id": 20,
                "description": None,
                "display": "row",
                "name": "Facebook Ads per Ads ID",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 143,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 102,
                "collection_id": 32,
                "description": None,
                "display": "line",
                "name": "Fact Daily Event Performance by Timestamp",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 180,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 103,
                "collection_id": 32,
                "description": None,
                "display": "line",
                "name": "Total Purchase Amount over time",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 181,
                "dashboard_id": 13,
                "parameter_mappings": []
            },
            {
                "id": 106,
                "collection_id": 32,
                "description": None,
                "display": "bar",
                "name": "Timestamp by quarter of the year",
                "include_csv": False,
                "include_xls": False,
                "dashboard_card_id": 182,
                "dashboard_id": 13,
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
                        "last_login": "2023-10-22T15:44:19.316944Z",
                        "is_active": True,
                        "is_qbnewb": False,
                        "updated_at": "2023-10-23T06:03:42.459196",
                        "is_superuser": True,
                        "login_attributes": None,
                        "id": 1,
                        "last_name": "Muhidin",
                        "date_joined": "2023-10-14T03:20:40.521886Z",
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
            "skip_if_empty": False,
            "collection_id": None,
            "parameters": [],
            "dashboard_id": 13
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
